<?php

declare(strict_types=1);

namespace IDCT\NATS\Connection;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\CompositeCancellation;
use Amp\DeferredCancellation;
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
use IDCT\NATS\Exception\NatsThrowable;
use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Exception\SlowConsumerException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Protocol\Enum\ProtocolFrameType;
use IDCT\NATS\Protocol\ProtocolCodec;
use IDCT\NATS\Protocol\ProtocolFrame;
use IDCT\NATS\Protocol\ProtocolParser;
use IDCT\NATS\Protocol\ServerInfo;
use IDCT\NATS\Transport\CancellableDialTransportInterface;
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
     * connection stays Connecting and subscribe/request/flush/rtt/processIncoming all wait out their
     * budget, or throw when waiting for a reconnect is disabled). After
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
     * Sids whose delivery a pass stopped at a handler failure it held to throw ({@see drainPendingForSid()}, #177),
     * the rest of their queue left for the next pass: your own next read continues them before it reads
     * ({@see deliverStoppedSubscriptions()}, #186). A subset of {@see $pendingDirty}, cleared where that one is: a sid
     * leaves the set when its queue empties, or with the subscription. A sid is marked when a pass stops it, not when
     * that pass ends: a read in another fiber can continue it while the read that stopped it is still held up in
     * another subscription's handler that awaits, and throw its next failure before that read throws the first. The
     * order of each subscription's deliveries is unchanged.
     *
     * @var array<int, true>
     */
    private array $stoppedByAFailure = [];
    /**
     * Muxed request inbox base subject for the current connection epoch (e.g. "_INBOX.<24hex>"), null
     * until the first request establishes it. One long-lived wildcard subscription "<base>.*" serves
     * EVERY request()/requestMany() reply instead of a fresh inbox + SUB/UNSUB per request (#118).
     * Retained across reconnect; cleared by releaseRuntimeState(), so a fresh connect() starts a new
     * random base, when the server rejects the subscription (#167, {@see dropUnconfirmedMux()}), and when
     * its set-up fails.
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
     * The -ERR is async: it lands after ensureMuxInbox() recorded the (now dead) muxSid/muxBase, which it
     * does before the SUB is written, so whichever fiber reads the -ERR finds them. On detection the mux
     * state is dropped (so a reconnect does not replay the rejected SUB) and every request()/requestMany()
     * then fails fast with a clear, catchable error instead of a silent permanent timeout. Reset by
     * releaseRuntimeState() so a fresh connect() re-attempts the mux inbox.
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
     * The wake-up of each in-flight request's waits, keyed like {@see $muxWaiters}: request()'s fires with its reply,
     * requestMany()'s with each reply, which rotates it ({@see renewMuxWake()}). The request reads with it, and waits
     * with it for the read slot while another fiber's read holds the socket. The connection fires every one of them
     * when it drops or rejects the reply inbox ({@see wakeMuxWaiters()}, #180), so that a request whose read is parked
     * on the socket, or which waits for the read slot, while another fiber's read met the -ERR looks again at once,
     * instead of with the server's next bytes, when that read ends, or at its deadline. Registered and removed with
     * the waiter, so it holds nothing once a request ends.
     *
     * @var array<string, DeferredCancellation>
     */
    private array $muxWakes = [];
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
     * Whether the server is known to hold the mux subscription: the PONG of the PING written right behind its
     * SUB has arrived ({@see $muxFence}), or something was delivered on it. Reset whenever its SUB is written,
     * by a first request or by a reconnect's replay. Until then a 'maximum subscriptions exceeded' -ERR, which
     * names no subject, may be the one rejecting that SUB, and the client drops the mux rather than trust it
     * ({@see dropUnconfirmedMux()}); once the mux is confirmed, such an -ERR is another SUB's.
     */
    private bool $muxConfirmed = false;
    /**
     * The pong slot of the PING written in the same write as the mux SUB, right behind it. The server answers
     * a SUB it rejects with an -ERR ahead of that PING's PONG, so the PONG confirms the mux. Null once it has
     * arrived, or once the mux it was written for is gone.
     *
     * @var DeferredFuture<null>|null
     */
    private ?DeferredFuture $muxFence = null;
    /**
     * Bumped whenever the client drops the mux subscription ({@see dropUnconfirmedMux()}), so that a request
     * waiting on the dropped one, or about to be sent on it, fails fast instead of waiting out its timeout.
     */
    private int $muxGeneration = 0;
    /**
     * Set when the client drops the mux subscription, cleared once a mux is confirmed: meanwhile a request
     * waits for the mux it is about to be sent on to be confirmed, so that while the server keeps rejecting
     * the SUB, requests fail without being sent.
     */
    private bool $awaitMuxFence = false;
    /**
     * Sids of dropped mux subscriptions, unsubscribed in the write that subscribes the next one: when the
     * -ERR was another SUB's after all, the server still holds the dropped one, and the UNSUB frees its slot
     * before the new SUB is counted. The server ignores an UNSUB for a sid it does not hold.
     *
     * @var list<int>
     */
    private array $muxSidsToRelease = [];
    /**
     * Counts terminal closes ({@see releaseRuntimeState()}), so that a request that was setting the mux up when
     * one happened reports the closed connection, even when a connect() has opened a new one since, and so that a
     * request waiting for its reply ends with the closed connection at its next look, whatever woke it (#180).
     */
    private int $terminalCloses = 0;
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
    /**
     * Sids subscribed through {@see subscribeGuarded()}: the reply inboxes of the JetStream pull fetch, the pull
     * pipeline and the batched Direct Get, whose rejection by the subscription limit must reach the operation
     * waiting on them, whichever fiber's read meets the -ERR (#175). Each gets the reply inbox's rule: a PING is
     * written right behind its SUB, and the sid is unconfirmed ({@see $unconfirmedSids}) until that PING's PONG or a
     * delivery on it. {@see resubscribeAll()} replays them the same way. Cleared with the sid's state.
     *
     * @var array<int, true>
     */
    private array $guardedSids = [];
    /**
     * The guarded sids the server is not yet known to hold, each with the pong slot of the PING written behind its
     * SUB ({@see $muxFence} for the reply inbox's). The server answers a SUB it rejects ahead of that PONG, and a
     * rejected SUB delivers nothing, so until the PONG or a delivery arrives a 'maximum subscriptions exceeded' -ERR,
     * which names no subject, may be the one rejecting the SUB: such an -ERR rejects every sid still here
     * ({@see rejectUnconfirmedSubscriptions()}). A sid leaves the map when it is confirmed or rejected, and with
     * its state; a confirmed sid costs nothing afterwards.
     *
     * @var array<int, DeferredFuture<null>>
     */
    private array $unconfirmedSids = [];
    /**
     * Sids of guarded subscriptions the client treated as rejected whose UNSUB is still to be written: when the -ERR
     * was another SUB's after all, the server still holds the subscription, and the UNSUB frees its slot. Written
     * on the connection the -ERR was read on, when the operation that owned the sid unsubscribes it
     * ({@see unsubscribe()}, {@see releaseRejectedSubscription()}), or by the replay for a replayed sid the new
     * server rejected ({@see finishReplayWindow()}). A sid still queued once that connection is gone is forgotten
     * ({@see connectOnce()}): the server dropped whatever it held with the connection. The server ignores an UNSUB
     * for a sid it does not hold.
     *
     * @var list<int>
     */
    private array $guardedSidsToRelease = [];
    private int $outstandingPings = 0;
    private ?string $pingTimerId = null;
    /**
     * FIFO pong-correlation queue (#117, nats.go `nc.pongs` parity): every outbound PING except
     * the connect handshake's enqueues one slot, in the step that hands the PING to the transport
     * ({@see enqueuePongSlot()}), and the PONG handler completes the OLDEST slot - TCP delivers
     * PONGs in PING order, so head-of-queue is exactly the PING this PONG answers.
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
     * The listeners called during the recovery - Disconnected, the Closed of a recovery that gives up, the
     * error listener, say - run synchronously inside it, so a listener-initiated connect() that joined the
     * recovery would await a deferred its own fiber must complete - a permanent deadlock. connect()
     * compares its caller's fiber against this to refuse such a join loudly (#145). Connected and
     * Reconnected are announced once the recovery is over ({@see announceOpen()}).
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
     * The wake-up of each subscription an operation's read waits on ({@see nextDeliveryTo()}). The next delivery to
     * the sid, whichever fiber makes it, cancels it and removes it; so do the subscription's removal and a terminal
     * close. Created when an operation reads for its own subscription, and shared by every read made for that sid
     * until it fires. A read keeps the one it was given: a cancellation fires at most once and is never reset, and the
     * next read gets a new one, so no later state can match the one a read started from (#174).
     *
     * @var array<int, DeferredCancellation>
     */
    private array $deliveryWakes = [];
    /**
     * The wake-ups a terminal close released while a reconnect was still in flight, kept until that reconnect has
     * published its outcome ({@see recoverConnection()}). Fired at the close, they woke an operation waiting for the
     * reconnect before its error was out whenever a listener the close calls suspended, and the operation went on to
     * fail with "Connection is not open" instead of the reconnect's own error. Kept referenced, since a
     * DeferredCancellation cancels itself when it is destroyed.
     *
     * @var list<DeferredCancellation>
     */
    private array $wakesAfterRecovery = [];
    /**
     * The wake-up of each pong slot a read waits for ({@see wakeOnPong()}): one per slot, whichever reader asks, so that
     * a flush, a drain's flush or a mux confirmation that reads many chunks before its PONG does not pile a callback per
     * read onto the slot. A slot that completed is forgotten with its last reader.
     *
     * @var \WeakMap<DeferredFuture<null>, Cancellation>|null
     */
    private ?\WeakMap $pongWakes = null;
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
     * Set for the whole of a {@see drain()}, including the parts where the state is not Draining: the
     * wait for an in-flight reconnect, and a drain that winds down without a connection because the
     * reconnect outlasted its budget. connect() refuses while it is set - the drain's teardown would
     * destroy a connection opened meanwhile - and a second drain() fails.
     */
    private bool $drainInProgress = false;
    /**
     * What a {@see drain()} waits for besides the connection's own backlog ({@see DrainParticipant}), by the id
     * {@see addDrainParticipant()} gave each: the JetStream pull consumer runs, each from the moment its inbox is
     * subscribed to the end of the run (#207). A drain asks each of them once its flush is done and waits for their
     * hand-overs within its budget. Not part of the per-connection state a close releases: a participant removes
     * itself.
     *
     * @var array<int, DrainParticipant>
     */
    private array $drainParticipants = [];
    /** The id {@see addDrainParticipant()} gave last. */
    private int $lastDrainParticipantId = 0;
    /**
     * Whether the {@see drain()} under way still hands over what the connection's participants hold
     * ({@see isDiscardingUndelivered()}, #207): set as the drain enters Draining, and cleared once its delivery phase is
     * over (every hand-over done, or its budget run out, before its deadline report) and by a disconnect() that
     * interrupts it. The state alone cannot say it: the drain's teardown and a disconnect() both await the transport's
     * close with the state still Draining, and a handler that went on meanwhile got what the drain had just reported as
     * discarded, or what the disconnect() was discarding.
     */
    private bool $drainDelivers = false;
    /**
     * Whether the current close has been announced with a Closed event, or is about to be: by the
     * disconnect() that set it, or by the connect or reconnect that gave up on the connection
     * ({@see markClosedForGood()}). Both set it before their awaited transport close, so whatever closes
     * the connection meanwhile ends quietly. drain() announces the close it performed only when nothing
     * else did (a reconnect that gave up, a disconnect() issued during the drain), so every close is
     * announced once. Cleared on a fresh connect().
     */
    private bool $closedAnnounced = false;
    /**
     * How many Connected or Reconnected listener calls are running. A reconnect that opens the connection while
     * one runs - typically one that an operation called from that listener had to start - has its own
     * announcement delivered from the event loop instead of from the operation ({@see announceOpen()}), so that
     * listener calls do not nest one level deeper with every new connection a flapping server drops.
     */
    private int $openListenerCalls = 0;
    /**
     * A connection a reconnect opened that the connection listener has yet to hear about: the event announcing
     * it and the connection's {@see $connectionGeneration}. Set by {@see announceOpen()} until the listener is
     * called. When that connection is lost first, the listener hears of neither the open nor the loss
     * ({@see performRecovery()}).
     *
     * @var ?array{event: ConnectionEvent, generation: int}
     */
    private ?array $unannouncedOpen = null;
    /**
     * Cancels the backoff delay of the reconnect in progress, so close-intent stops a reconnect at once
     * instead of when its current delay ends ({@see setCloseIntent()}).
     */
    private ?DeferredCancellation $reconnectBackoff = null;

    /**
     * Stops the dial in progress - a connect()'s or a reconnect's - when the user closes the connection
     * ({@see setCloseIntent()}), for a transport that can stop its dial
     * ({@see CancellableDialTransportInterface}).
     */
    private ?DeferredCancellation $dialStop = null;
    /**
     * Counts the connections this client has made: bumped by every handshake attempt. A read, write or
     * heartbeat captures it before its I/O and hands it to {@see recoverConnection()} when that I/O
     * fails, so a failure from a connection the application has since closed and reopened cannot tear
     * down the new, healthy one.
     */
    private int $connectionGeneration = 0;
    /**
     * Sids drainSubscription() left to the delivery already under way for them: drainPendingForSid()
     * removes the subscription once it has delivered everything queued (see drainSubscription()).
     *
     * @var array<int, true>
     */
    private array $removeAfterDelivery = [];
    /**
     * Overflows already reported through the error listener, so none is reported twice. With
     * {@see NatsOptions::$slowConsumerErrorsFailOperations} a SubscriptionQueue's overflow is reported and
     * then thrown; a read that goes on to report what it caught - because a failure of its own takes
     * precedence - must not report it again. Weak: an entry goes away with its exception.
     *
     * @var \WeakMap<SlowConsumerException, true>
     */
    private \WeakMap $reportedOverflows;
    /**
     * The failures of frames that end the connection ({@see frameFailureEndsConnection()}): a fatal -ERR, which
     * the server sends right before it closes the connection, and, outside a drain, a PONG the socket would not
     * take. Marked where they are raised, so that no other failure a frame raises can end the connection. Weak:
     * an entry goes away with its exception.
     *
     * @var \WeakMap<\Throwable, true>
     */
    private \WeakMap $connectionEndingFailures;
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
     * The lame-duck failover that a read with a wake-up started in a fiber of its own (#191), with the
     * {@see $connectionGeneration} of the connection it leaves ({@see startLameDuckFailover()}): such a read waits
     * for it only within the operation's own wait ({@see awaitLameDuckFailoverFrom()}), and the rest of any chunk
     * read on that connection is the leaving server's ({@see dispatchFrames()}). Cleared when the failover returns.
     *
     * @var array{generation: int, failover: Future<void>}|null
     */
    private ?array $lameDuckFailover = null;
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
        $this->reportedOverflows = new \WeakMap();
        $this->connectionEndingFailures = new \WeakMap();
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
     * Bounded by one request-timeout budget. While a reconnect is in flight it first waits for it within
     * that budget ({@see NatsOptions::$waitForReconnect}); the wait is not part of the measured time.
     *
     * @return Future<float> Round-trip time in seconds.
     */
    public function rtt(): Future
    {
        $caller = \Fiber::getCurrent();

        return async(function () use ($caller): float {
            $deadline = $this->monotonicSeconds() + max(0.1, $this->options->requestTimeoutMs / 1000);

            if ($this->state !== ConnectionState::Open) {
                try {
                    $this->awaitOpenConnection($this->remainingBudgetCancellation($deadline), $caller);
                } catch (CancelledException) {
                    throw new TimeoutException('RTT measurement timed out waiting for the connection to be re-established');
                }
            }

            $start = $this->monotonicSeconds();
            $this->flushWithin(
                $deadline,
                $caller,
                reportOverflows: !$this->options->slowConsumerErrorsFailOperations,
                reportHandlerFailures: !$this->options->handlerErrorsFailOperations,
            );

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
            // Checked first: a drain waiting for a reconnect, or winding down after its budget ran out,
            // is not in the Draining state, and its teardown would destroy a connection opened now.
            if ($this->drainInProgress) {
                throw new ConnectionException('Cannot connect: drain in progress');
            }

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

                // A recovery that disconnect()/drain() stopped never opens the connection, so joining it
                // could only fail - and would hang for good when this caller is itself awaited, however
                // indirectly, by that recovery: a Closed listener that reconnects after a drain() which
                // the recovery's own listener awaits. The close wins over this connect() either way (#145).
                if ($this->closing) {
                    throw new ConnectionException('Recovery was aborted before the connection opened');
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
            $this->closedAnnounced = false;

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

            // A success, direct or through the owned recovery hand-off, already settled $connecting before
            // emitting Connected; it is still pending here only when a close stopped the hand-off.
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
     * Waits for an in-flight reconnect before an operation that needs an open connection, instead of
     * failing the operation at once ({@see NatsOptions::$waitForReconnect}).
     *
     * Failing on the spot starved synchronous applications. A recovery only advances while something
     * waits on the event loop - its dials, handshake reads and backoff timers are loop callbacks - and
     * awaiting an operation that fails immediately never gets the loop past its microtask phase. A
     * long-running synchronous process (a queue worker, a daemon) whose operations all failed that way
     * left a recovery the heartbeat had started in the background parked for good: the state stayed
     * Connecting and every operation failed until the process restarted. Awaiting the PENDING recovery
     * future suspends the caller for real, which is what lets the loop run the recovery to its end.
     *
     * Loops, because a finished recovery can be followed by another one before the caller resumes.
     * Refuses - the fail-fast behaviour - when waiting is disabled, when the user is closing the
     * connection, when no recovery is in flight (Idle, or terminally Closed: nothing will reopen the
     * connection), and when the caller IS the recovery fiber: a listener called during the recovery (a
     * Disconnected or error listener) runs inside it, and waiting there would await a deferred only that
     * suspended fiber can complete (#145). A recovery that a failed initial connect() hands off to runs in
     * the connect fiber, which is then the recovery fiber; any other caller - a Connected or Reconnected
     * listener included, which runs once the recovery is over - can wait safely. A
     * recovery that fails surfaces its own error (e.g. "Reconnect attempts exhausted"), exactly as a
     * joined connect() does.
     *
     * The wait is bounded only by $cancellation. Callers derive it from their own single budget, so the
     * reconnect wait and the operation itself share one timeout.
     *
     * @param ?\Fiber<mixed, mixed, mixed, mixed> $caller The fiber that issued the operation, captured
     *        before the operation's own async() fiber was spawned (null for {main}).
     * @param bool $acceptDraining Whether a Draining connection is usable (the read path's rule).
     *
     * @throws ConnectionException When the connection is not open and there is nothing to wait for.
     * @throws CancelledException When $cancellation fires first; callers map it to their timeout error.
     */
    private function awaitOpenConnection(?Cancellation $cancellation, ?\Fiber $caller, bool $acceptDraining = false): void
    {
        while (!$this->isUsableForOperations($acceptDraining)) {
            $recovery = $this->reconnecting;
            if (
                $recovery === null
                || !$this->options->waitForReconnect
                || $this->closing
                || ($caller !== null && $caller === $this->recoveryFiber)
            ) {
                throw new ConnectionException('Connection is not open');
            }

            $recovery->getFuture()->await($cancellation);
        }
    }

    /**
     * Whether the connection currently accepts operations: Open, or also Draining for the read path.
     */
    private function isUsableForOperations(bool $acceptDraining): bool
    {
        return $this->state === ConnectionState::Open
            || ($acceptDraining && $this->state === ConnectionState::Draining);
    }

    /**
     * Gives the event loop one tick after a publish was buffered during a reconnect
     * ({@see NatsOptions::$waitForReconnect}). Buffering returns without ever suspending, so a
     * synchronous publisher that only buffered would never let the loop run the recovery that flushes
     * the buffer: the recovery stayed parked until the buffer filled and every later publish threw.
     * The frame is already buffered in order, so yielding here cannot reorder anything.
     */
    private function yieldToInFlightRecovery(): void
    {
        if ($this->options->waitForReconnect) {
            delay(0);
        }
    }

    /**
     * Recovers a connection whose socket failed a bounded write - a request's PUB/HPUB, a SUB, or the
     * PING of flush() or rtt() - and waits for it the way an operation waits for a recovery already in flight
     * ({@see awaitOpenConnection()}): within the caller's own budget, and not at all when waiting is
     * disabled. A recovery that outlasts the budget carries on without the caller. A reconnect that the
     * recovery's Reconnected listener leaves in flight is waited for within the same budget.
     *
     * Such a write used to leave the connection Open on the dead socket, its raw error reaching the caller,
     * and every operation that wrote a control frame failed the same way until the heartbeat noticed. A
     * plain publish recovers inline and sends again; these hand the recovery to its own fiber, as the heartbeat
     * does, so that a caller with a budget keeps to it. It joins a recovery another fiber already runs, and
     * starts none for a connection since replaced ({@see recoverConnection()}). With reconnect off it closes
     * the connection for good, and the caller gets its "Reconnect is disabled".
     *
     * @param int $generation The {@see $connectionGeneration} the failed write ran on.
     * @param ?\Fiber<mixed, mixed, mixed, mixed> $caller The fiber that issued the operation.
     *
     * @throws ConnectionException When the connection does not come back, or at once when waiting is disabled.
     * @throws CancelledException When $cancellation fires first; callers map it to their timeout error.
     */
    private function recoverAfterFailedWrite(int $generation, \Throwable $writeError, Cancellation $cancellation, ?\Fiber $caller): void
    {
        $recovery = async(function () use ($generation, $writeError): void {
            $this->recoverConnection(failedGeneration: $generation, cause: $writeError);
        });
        // A recovery nobody waits for fails on its own terms: the connection closes, and says so.
        $recovery->ignore();

        if (!$this->options->waitForReconnect) {
            throw new ConnectionException('Connection is not open', 0, $writeError);
        }

        $recovery->await($cancellation);

        if ($this->state !== ConnectionState::Open) {
            try {
                // The recovery announced the new connection before it ended, and the listener, or what it
                // called, may have started another reconnect since: waited for within the same budget, like
                // any reconnect in flight.
                $this->awaitOpenConnection($cancellation, $caller);
            } catch (ConnectionException) {
                // Nothing to wait for, or it did not reopen the connection: the user is closing it, a connect()
                // owns the dial, or the connection was closed for good.
                throw new ConnectionException('Connection is not open', 0, $writeError);
            }
        }
    }

    /**
     * Whether a frame whose dispatch failed means the connection is finished: a fatal -ERR, which the server
     * sends right before it closes the socket, or, outside a drain, a PONG the socket would not take. Nothing
     * else does: not an -ERR the server sends while keeping the connection open, such as the one rejecting a
     * SUB beyond the maximum subscriptions ({@see isServerErrorKeepingTheConnectionOpen()}), which since 2.10.1
     * had ended healthy connections; not a full subscription queue, where one subscriber fell behind; not
     * anything else a frame throws. During a drain a PONG the socket would not take ends the drain's flush
     * instead ({@see handleFrame()}): the connection is closing anyway.
     */
    private function frameFailureEndsConnection(\Throwable $failure): bool
    {
        return ($this->connectionEndingFailures[$failure] ?? false) === true;
    }

    /**
     * Whether $failure, which a read of this connection threw, is the failure of a frame that ended the connection
     * ({@see frameFailureEndsConnection()}): a fatal -ERR, such as 'Stale Connection' or 'User Authentication
     * Expired', which the server sends right before it closes the connection, or, outside a drain, a PONG the socket
     * would not take. The read that met such a frame recovered the connection before it threw (#171), within its own
     * wait when waiting for a reconnect is enabled: by the time the caller sees the failure, the connection can be
     * open again on a new socket, still reconnecting, or closed for good, and the failure says only why the old one
     * ended. Every other failure answers false: an -ERR the server keeps the connection open for, a full
     * subscription queue, a handler's exception, and what a read throws when the socket itself went (the connection
     * reports the socket's own error as it starts the reconnect).
     *
     * The one classifier of such a failure: the library's operations that outlive a connection ask it, through
     * NatsClient, to tell a failure that ended the connection from one that ended only their read. The pull
     * consumer engine does, whose infinite run goes on after such a failure as it goes on after a lost connection
     * ({@see \IDCT\NATS\JetStream\JetStreamContext::consumePipelined()}, #210).
     *
     * @internal Low-level mechanism for the library's own operations; not general API.
     */
    public function endedTheConnection(\Throwable $failure): bool
    {
        return $this->frameFailureEndsConnection($failure);
    }

    /**
     * Recovers a connection that a frame said was finished ({@see frameFailureEndsConnection()}), so that the
     * read that met the frame is the only operation to fail (#171). Left Open, the connection had the next
     * operation write into the socket the server had closed, where it failed as well, or with reconnect on
     * waited out its whole timeout first.
     *
     * The recovery runs in its own fiber, like the one a failed control write starts, and the read waits for
     * it only within its own budget, the way it waits for a reconnect already in flight. With reconnect off
     * the recovery just closes the connection for good, so the caller's next operation finds it Closed. With
     * reconnect on, a read that may not wait for a reconnect ({@see NatsOptions::$waitForReconnect}) does
     * not wait for this one: the recovery has left Open before any later operation runs, since it was queued
     * first. It joins a recovery another fiber already runs, and starts none for a connection since replaced
     * or one the user is closing ({@see recoverConnection()}).
     *
     * The read then fails with the frame's own error, which says why the connection ended. How the recovery
     * ends - reopened, closed because reconnect is off, given up - is announced by the connection events.
     *
     * @param int $generation The {@see $connectionGeneration} the frame was read on.
     * @param \Throwable $frameError The frame's error, the cause of the recovery's own failure (#172).
     */
    private function recoverAfterEndingFrame(int $generation, \Throwable $frameError, ?Cancellation $cancellation): void
    {
        $recovery = async(function () use ($generation, $frameError): void {
            $this->recoverConnection(failedGeneration: $generation, cause: $frameError);
        });
        // A recovery nobody waits for fails on its own terms: the connection closes, and says so.
        $recovery->ignore();

        if ($this->options->reconnectEnabled && !$this->options->waitForReconnect) {
            return;
        }

        try {
            $recovery->await($cancellation);
        } catch (\Throwable) {
            // Closed for good (reconnect off, attempts used up, credentials refused), or the read's own
            // deadline ended the wait while the recovery carries on: either way the caller gets the frame's
            // error.
        }
    }

    /**
     * Recovers a connection whose socket failed the read of one of the library's operations - a read with a
     * wake-up ({@see readChunk()}): the read of an operation that waits for a result of its own, or a flush's
     * read for its PONG - and waits for it the way the operation waits for a reconnect another fiber runs:
     * within the operation's own wait. With waiting disabled it does not wait at all, and the operation fails
     * at once with "Connection is not open", the read's error its cause, as one whose control write noticed
     * the loss does (#178).
     *
     * Such a read used to run the recovery inline, as your own processIncoming() still does, so neither the
     * operation's deadline nor its wake-up could end the wait: with the server down for two seconds, a request
     * with a one-second timeout returned after two, and so did a poll whose message a delivery still under way
     * brought during the outage. The recovery runs in its own fiber instead, like the one a failed control write
     * or a connection-ending frame starts ({@see recoverAfterFailedWrite()}, {@see recoverAfterEndingFrame()}),
     * and $waitCancellation, the operation's deadline composed with its wake-up, bounds the wait: the deadline
     * ends it with the caller's cancellation, so that the operation times out, and the wake-up ends it with the
     * read returning, so that the operation looks again. Either way the recovery carries on, announces the new
     * connection once it is over, and fails on its own terms when it gives up. A recovery that ends while the
     * read waits ends the wait as well, with its outcome: the read returns and the operation looks again before
     * it reads the new connection, as after a reconnect it joined, or gets the recovery's error. A recovery
     * another fiber already runs is joined within the same wait, as before.
     *
     * The recovery fiber is then the one started here, not the read's: a listener called during the recovery
     * runs inside it, and an operation issued from there is refused a join, as for a failed write
     * ({@see awaitOpenConnection()}).
     *
     * @param int $generation The {@see $connectionGeneration} the failed read ran on.
     * @param \Throwable $readError The read's error, the cause of the recovery's own failure (#172).
     * @param Cancellation|null $waitCancellation What the read waits with: the caller's cancellation and the
     *        read's wake-up, whichever are given.
     *
     * @throws CancelledException When $waitCancellation fires first, or had fired by the time the read's error was
     *         reported; the read tells the deadline from the wake-up.
     * @throws ConnectionException When the recovery gives up, or closes the connection because reconnect is off;
     *         at once, with $readError as its previous, when waiting for a reconnect is disabled.
     */
    private function recoverAfterFailedOperationRead(int $generation, \Throwable $readError, ?Cancellation $waitCancellation): void
    {
        if ($this->reconnecting !== null) {
            $this->recoverConnection(joinCancellation: $waitCancellation, failedGeneration: $generation, cause: $readError);

            return;
        }

        $recovery = async(function () use ($generation, $readError): void {
            $this->recoverConnection(failedGeneration: $generation, cause: $readError);
        });
        // A recovery nobody waits for fails on its own terms: the connection closes, and says so.
        $recovery->ignore();

        // Waiting disabled: the operation fails at once, as every operation does while a reconnect is in flight,
        // rather than find the connection not open at its next read. A flush that read on instead met either
        // the PONG the recovery's first attempt fails or the closed connection, depending on how long the
        // transport's close took. Not when the operation's wait ended while the read's error was reported:
        // what it waits for came meanwhile, in a delivery still under way, or its deadline passed, and the read
        // tells which from the cancellation.
        if ($this->options->reconnectEnabled && !$this->options->waitForReconnect) {
            $waitCancellation?->throwIfRequested();

            throw new ConnectionException('Connection is not open', 0, $readError);
        }

        $recovery->await($waitCancellation);
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
        } catch (AuthenticationException $e) {
            $this->abandonConnectIfClosed($e);

            // An auth failure will not resolve by retrying: fail fast instead of entering reconnect.
            $this->markClosedForGood();
            $this->closeTransportBestEffort();
            $this->releaseRuntimeState();
            // Settle before emitting Closed so the deferred is never pending under a listener (#145).
            $this->settleConnecting($e);
            $this->emitEvent(ConnectionEvent::Closed, $e);

            throw $e;
        } catch (\Throwable $e) {
            $this->abandonConnectIfClosed($e);

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

            $this->markClosedForGood();
            $this->closeTransportBestEffort();
            $this->releaseRuntimeState();
            // Settle before emitting Closed (deferred never pending under a listener, #145) and with
            // the SAME wrapped exception the direct caller receives, so joiners see one error type.
            $wrapped = new ConnectionException($e->getMessage(), (int) $e->getCode(), $e);
            $this->settleConnecting($wrapped);
            $this->emitEvent(ConnectionEvent::Closed, $e);
            throw $wrapped;
        }

        $this->abandonConnectIfClosed();

        $this->markConnectionOpen();
        // Settle $connecting before running the listener: the deferred must never be pending while user
        // code runs, or a listener-initiated connect() join would await an outcome only this (now
        // suspended-in-the-listener) fiber can produce - a deadlock - and a concurrent live-epoch failure
        // could be swallowed by recoverConnection()'s guard (#145).
        $this->settleConnecting(null);
        $this->emitEvent(ConnectionEvent::Connected);
    }

    /**
     * Ends the connect() in progress when disconnect() overtook it while it was dialling
     * ({@see abandonConnectAfterClose()}); otherwise does nothing.
     *
     * @phpstan-impure Reads close-intent, which disconnect() sets while this connect is suspended.
     */
    private function abandonConnectIfClosed(?\Throwable $cause = null): void
    {
        if ($this->closing) {
            $this->abandonConnectAfterClose($cause);
        }
    }

    /**
     * Ends a connect() that disconnect() overtook while it was dialling: the close wins (#145), so the
     * socket this connect opened is closed again, nothing reconnects, and - disconnect() having announced
     * the close - no second Closed event is emitted. The caller gets the failure a connect() that joins a
     * stopped reconnect gets. Without this the dial went Open behind the close: a connection that
     * reported Closed, with close-intent still set so that nothing would ever recover it.
     */
    private function abandonConnectAfterClose(?\Throwable $cause): never
    {
        $this->state = ConnectionState::Closed;
        $this->closeTransportBestEffort();
        $aborted = new ConnectionException('Connect was aborted before the connection opened', 0, $cause);
        $this->settleConnecting($aborted);

        throw $aborted;
    }

    /**
     * Marks the connection Closed for good on a path that gave up on it - a connect or a reconnect that
     * failed for good - and claims the Closed event that path emits once its clean-up is done. Claimed
     * now, before the clean-up's awaited transport close: a disconnect() issued meanwhile, or a drain()
     * whose budget runs out meanwhile, then ends quietly instead of announcing the same close again.
     */
    private function markClosedForGood(): void
    {
        $this->state = ConnectionState::Closed;
        $this->closedAnnounced = true;
    }

    /**
     * Resolves and clears the in-flight connect() deferred exactly once. Called before every
     * synchronous lifecycle emission on a direct performConnect() exit path so the deferred is never
     * pending while user code (a connection/error listener) runs: a pending $connecting under a
     * listener re-opens the join deadlock (a listener's connect() awaiting a deferred only the
     * suspended emitting fiber can complete) and the swallowed-recovery window (#145). The
     * owned-recovery hand-off settles it before its Connected as well ({@see recoverConnection()}).
     * Idempotent - connect() settles it again after performConnect() returns, for a hand-off that a
     * close stopped or that failed.
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
     * {@see drain()} for the lossless path: it delivers the buffered backlog before closing. What a
     * JetStream pull consumer run's pulls hold is discarded the same way, wherever the run is
     * ({@see isDiscardingUndelivered()}, #207).
     *
     * @return Future<void>
     */
    public function disconnect(): Future
    {
        // Captured on the CALLER's fiber, as connect() does: a disconnect() issued from a listener inside the
        // reconnect it stops must not wait for that reconnect ({@see awaitStoppedDials()}).
        $caller = \Fiber::getCurrent();

        return async(function () use ($caller): void {
            // A close already announced - by a drain(), a reconnect that gave up, an earlier (or a
            // concurrent) disconnect() - is not announced again: this closes quietly (drain() then
            // disconnect() is a common shutdown idiom).
            $announce = !$this->closedAnnounced;

            // Signal close-intent BEFORE closing the socket so an in-flight reconnect/heartbeat read
            // cannot race to re-open the connection after the user asked to close it (#84).
            $this->setCloseIntent();
            // A drain() this interrupts hands nothing more over, also while the close below is awaited with the
            // state still Draining (#207).
            $this->drainDelivers = false;
            $stopped = $this->stoppedDials();
            // This disconnect announces the close below: a drain() it interrupts must not announce it
            // again when it ends, even if it ends first.
            $this->closedAnnounced = true;
            $this->transport->close()->await();
            $this->state = ConnectionState::Closed;

            // Release per-connection state so a long-lived/pooled client (or one disconnect()ed and
            // later reused) does not retain handler closures, buffered messages, parser bytes, or the
            // reconnect buffer until the whole object is GC'd (#85). Mirrors drain()'s teardown.
            $this->releaseRuntimeState();

            if ($announce) {
                $this->emitEvent(ConnectionEvent::Closed);
            }

            $this->awaitStoppedDials($stopped, $caller);
        });
    }

    /**
     * Records the user's close-intent (disconnect()/drain()): an in-flight reconnect stops at its next
     * check instead of reopening the connection (#84), and its backoff delay is cut short so it stops at
     * once - rather than keep running, and keep a connect() that joins it waiting, until the delay ends.
     * The dial in progress, a reconnect's or a connect()'s, is stopped too where the transport can stop
     * it ({@see dialTransport()}).
     */
    private function setCloseIntent(): void
    {
        $this->closing = true;
        $this->cancelPingTimer();
        $this->reconnectBackoff?->cancel();
        $this->dialStop?->cancel();
    }

    /**
     * The reconnect and the connect() in progress, each with the fiber that runs it: what a close that has
     * just set close-intent stops, for {@see awaitStoppedDials()}. Taken before the close announces itself,
     * so the wait is not for a connect() that a Closed listener starts.
     *
     * @return list<array{DeferredFuture<void>, ?\Fiber<mixed, mixed, mixed, mixed>}>
     */
    private function stoppedDials(): array
    {
        $stopped = [];
        if ($this->reconnecting !== null) {
            $stopped[] = [$this->reconnecting, $this->recoveryFiber];
        }

        if ($this->connecting !== null) {
            $stopped[] = [$this->connecting, $this->connectFiber];
        }

        return $stopped;
    }

    /**
     * Waits for the reconnect or the connect() that a close stopped to end, so that disconnect() and drain()
     * resolve with nothing they stopped still running. A connect() issued after the close then dials afresh.
     * It used to meet the stopped reconnect still dialling, and fail with "Recovery was aborted before the
     * connection opened"; in a synchronous application that reconnect never got the event-loop time to end,
     * and every connect() failed that way.
     *
     * Bounded by the connect timeout, which is how long a transport that cannot stop its dial may take to
     * end it ({@see CancellableDialTransportInterface}); a connection listener could take any time at all.
     * Skipped for the fiber that runs what was stopped: a close called from a listener called during it (a
     * Disconnected or error listener) runs inside it, and waiting there would await what only that suspended
     * fiber can complete (#145). A Reconnected listener runs once the reconnect is over, and is not waited for.
     *
     * @param list<array{DeferredFuture<void>, ?\Fiber<mixed, mixed, mixed, mixed>}> $stopped From {@see stoppedDials()}.
     * @param ?\Fiber<mixed, mixed, mixed, mixed> $caller The fiber that called the close.
     */
    private function awaitStoppedDials(array $stopped, ?\Fiber $caller): void
    {
        if ($stopped === []) {
            return;
        }

        // A referenced timer, unlike a TimeoutCancellation's: what the stopped dial waits for may hold no watcher
        // that keeps the event loop running, and the close must still return at the bound.
        $bound = new DeferredCancellation();
        $timer = EventLoop::delay($this->options->connectTimeoutMs / 1000, static function () use ($bound): void {
            $bound->cancel();
        });
        foreach ($stopped as [$deferred, $fiber]) {
            if ($caller !== null && $caller === $fiber) {
                continue;
            }

            try {
                $deferred->getFuture()->await($bound->getCancellation());
            } catch (\Throwable) {
                // However it ended, or if it is still running at the bound, the close stands.
            }
        }

        EventLoop::cancel($timer);
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
        $this->stoppedByAFailure = [];
        $this->receivedCounts = [];
        $this->deliveredCounts = [];
        $this->autoUnsubMax = [];
        // Mux request inbox (#118): a terminal close ends the epoch. Clearing muxSid/muxBase forces a
        // fresh ensureMuxInbox() (new random base + sid) on the next connect(); clearing muxWaiters drops
        // any still-registered in-flight waiter - its request fiber terminates via the wait-loop state
        // gate / recovery exception, not via the map. muxTokenSeq is intentionally NOT reset (the new
        // random base makes any cross-epoch token undeliverable regardless). The waiters' wake-ups are
        // dropped with them, not fired (#180): nothing waits on past a terminal close - a read on the socket
        // fails with the closed transport, one parked for the read slot is released with it, one waiting for a
        // recovery that gave up fails with it - and each request's loop exits on the close count anyway
        // ({@see $terminalCloses}), as the one a rejection woke right before this close does.
        $this->forgetMux();
        $this->muxWaiters = [];
        $this->muxWakes = [];
        // A fresh connect() may target an account/server that permits the reply-inbox wildcard, so a
        // terminal close clears the rejection latch and lets ensureMuxInbox() re-attempt the mux (#167).
        $this->muxRejected = false;
        // The next connection's first mux starts afresh too: nothing to release, no fence to wait for, and no
        // set-up still under way for this one to join (it fails with the closed connection).
        $this->awaitMuxFence = false;
        $this->muxSidsToRelease = [];
        $this->muxInboxSetup = null;
        $this->terminalCloses++;
        $this->unboundedSids = [];
        $this->subscriptionRejectionHandlers = [];
        // The guarded inboxes went with their subscriptions, and the server dropped whatever it held with the
        // connection: nothing is left to confirm, reject or release (#175).
        $this->guardedSids = [];
        $this->unconfirmedSids = [];
        $this->guardedSidsToRelease = [];
        $this->removeAfterDelivery = [];
        $this->reconnectBuffer = '';
        $this->parser = new ProtocolParser();
        // Terminal close: no PONG will ever arrive for a queued PING, so parked flush/rtt waiters
        // must observe the close instead of idling out their deadlines (#117).
        $this->failPongWaiters(new ConnectionException('Connection closed before the server answered the PING'));
        // Nothing more will be delivered to the subscriptions released above: an operation's read still waiting for
        // one looks again, and finds the connection closed.
        $wakes = array_values($this->deliveryWakes);
        $this->deliveryWakes = [];
        if ($this->reconnecting !== null) {
            array_push($this->wakesAfterRecovery, ...$wakes);

            return;
        }

        foreach ($wakes as $wake) {
            $wake->cancel();
        }
    }

    /**
     * Emits a loud error naming the count of parsed-but-undelivered inbound messages a terminal close is
     * about to discard, mirroring the outbound reconnect-buffer discard (#123) so an inbound backlog
     * dropped at close is never silent - the same observable-drop principle as #134. Called BEFORE
     * releaseRuntimeState() clears the queues. drain() reports its own bounded-deadline discard (#149),
     * except when it cannot wait for a reconnect and closes instead, and disconnect() is the documented
     * lossy nats.go Close() path, so it does not route through here (#158).
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
     * Best-effort transport close for terminal failure exits, mirroring disconnect(), and before each
     * reconnect attempt dials. connectOnce() opens the socket before the handshake can fail, so every
     * path that gives up (terminal connect failure, exhausted or auth-aborted recovery) must close it -
     * otherwise the fd stays pinned by the transport until the client object itself is GC'd (#133).
     * Close failures are irrelevant here: the socket may already be gone, and the terminal state
     * transition, or the next attempt, is what matters.
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
     *
     * @param DeferredFuture<null>|null $pongSlot The pong slot of the PING that $bytes end with ({@see newPongSlot()}).
     *        The writer queues it right before it hands the bytes to the transport, so that its place in the queue is
     *        its PING's place on the wire ({@see enqueuePongSlot()}). A writer that runs after the caller stopped
     *        waiting still queues it with its PING, whose PONG is then owed to it; a write that fails takes it out
     *        again, since its PING never reached the wire.
     */
    private function writeBounded(string $bytes, Cancellation $cancellation, ?DeferredFuture $pongSlot = null): void
    {
        $write = async(function () use ($bytes, $pongSlot): void {
            if ($pongSlot !== null) {
                $this->pongWaiters[] = $pongSlot;
            }

            try {
                $this->transport->write($bytes)->await();
            } catch (\Throwable $writeError) {
                if ($pongSlot !== null) {
                    $this->discardPongSlot($pongSlot);
                }

                throw $writeError;
            }
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
     * One budget (~requestTimeoutMs) bounds the whole drain. While a reconnect is in flight the drain
     * first waits for it within that budget ({@see NatsOptions::$waitForReconnect}): the reconnect
     * flushes the publishes buffered during the outage, then the new connection is drained as usual. If
     * the budget runs out first the drain still ends Closed - it stops the reconnect, runs its usual
     * backlog pass and reports the buffered publishes it discards through the error listener. A drain
     * that cannot wait (waiting disabled, or called from a listener that runs inside the reconnect - a
     * Disconnected or error listener) closes the connection and throws, like nats.go's Drain() while
     * reconnecting, rather than leaving the reconnect to reopen a connection the application is shutting
     * down.
     *
     * Every drain ends with the connection Closed and one {@see ConnectionEvent::Closed} event - emitted
     * by the drain, or by what closed the connection before it could (a reconnect that gave up, a
     * disconnect() issued meanwhile). connect() is refused until the drain is over. A full subscription
     * queue (SlowConsumerPolicy::Error), a handler that throws, or an -ERR the server keeps the connection
     * open for, that the drain's flush runs into, is reported and does not end the flush.
     *
     * What a JetStream pull consumer run's pulls hold is delivered too (#207): the run is a drain participant
     * ({@see DrainParticipant}), and once the flush is done, while the connection is Draining, the drain asks it to
     * hand its pulls over to its handler and waits for that within the same budget, as it waits for its own backlog,
     * the handler's acks going out meanwhile, though not its requests, which the connection refuses while it is
     * Draining; the run then ends, and its handle() resolves with its count. A run used to lose them: they had left
     * the connection's queues long before, and the drain closed the connection under them. A drain whose budget runs
     * out first closes the connection all the same, and what the run still holds is discarded from the "drain deadline
     * exceeded" report on, which counts it, as a disconnect() discards it, also while the transport's close is under
     * way ({@see $drainDelivers}). A drain without a connection (the budget ran out before the reconnect it waited for
     * was done, or that reconnect gave up) asks no run: nothing was flushed, and a run whose read then fails discards
     * what it holds; nor does a drain whose flush a disconnect() cut short. A drain awaited from code the run calls
     * (its handler, its onError, the error listener while the run reports to it) waits for the very run that waits for
     * it, to the end of its budget, as one awaited from a subscription's handler waits for that handler.
     *
     * @return Future<void>
     */
    public function drain(): Future
    {
        $caller = \Fiber::getCurrent();

        return async(function () use ($caller): void {
            // Nothing to drain, and no side effects: a drain is already running, or the connection is not
            // open and no live reconnect will reopen it - Idle, Closed (even while the reconnect that just
            // gave up is still reporting it through a listener), or closing.
            if (
                $this->drainInProgress
                || ($this->state !== ConnectionState::Open
                    && ($this->reconnecting === null || $this->closing || $this->state === ConnectionState::Closed))
            ) {
                throw new ConnectionException('Connection is not open');
            }

            $this->drainInProgress = true;
            try {
                $this->drainConnection($caller);
            } finally {
                $this->drainInProgress = false;
                // A reconnect the drain stopped, when its budget ran out before the reconnect was done.
                $stopped = $this->stoppedDials();
                // Announced once the drain is over, so a Closed listener may connect() again at once - and
                // not a second time when the path that closed the connection already announced it. The
                // connection is Closed here however drainConnection() ended: every exit closes it.
                if (!$this->closedAnnounced) {
                    $this->emitEvent(ConnectionEvent::Closed);
                }

                $this->awaitStoppedDials($stopped, $caller);
            }
        });
    }

    /**
     * The body of {@see drain()}, once it is known there is something to drain.
     *
     * @param ?\Fiber<mixed, mixed, mixed, mixed> $caller The fiber that called drain().
     */
    private function drainConnection(?\Fiber $caller): void
    {
        // One overall drain budget (monotonic), computed once at entry, bounds every phase - the wait
        // for an in-flight reconnect, the flush-wait and the backlog-wait: total drain time cannot
        // exceed ~requestTimeoutMs. A second sequential deadline for a later phase would roughly
        // double worst-case drain latency; #149 requires the wait be bounded by the existing
        // (singular) drain deadline.
        $drainDeadline = $this->monotonicSeconds() + max(0.1, $this->options->requestTimeoutMs / 1000);

        // Not connected: the budget ran out before the reconnect finished, the reconnect gave up (it
        // reported what it discarded and closed the connection), or the user disconnect()ed meanwhile.
        // The drain then winds down without a connection - for the last two there is nothing left.
        $connected = $this->state === ConnectionState::Open || $this->awaitReconnectForDrain($drainDeadline, $caller);

        if ($connected) {
            $this->state = ConnectionState::Draining;
            $this->drainDelivers = true;
        }
        // Close-intent: a recovery triggered mid-drain must not re-open the connection (#84) - and one
        // still in flight when the budget ran out stops now, its backoff cut short.
        $this->setCloseIntent();
        // Recorded on the instance so mid-drain handler publishes (writePublishFrame's Draining branch)
        // and the per-sid delivery loop (drainPendingForSid) share this same budget.
        $this->drainDeadline = $drainDeadline;

        // Without a connection there is nothing to unsubscribe or flush: only the backlog pass and the
        // teardown below apply.
        if ($connected) {
            $this->unsubscribeAndFlushForDrain($drainDeadline);
        }

        // The drain's participants hold messages the connection handed them long before, which their handlers have not
        // got yet: the JetStream pull consumer runs, whose pulls are handed over only when they complete (#207). Once
        // the flush is done the server has stopped delivering to them and the drain has read what it sent before the
        // UNSUBs, so each is asked to hand over what it holds now, while the connection is Draining and the handlers'
        // acks still go out, and the loop below waits for those hand-overs as it waits for the backlog. They used to be
        // left out: the drain closed the connection under them, and a run whose read then failed discarded them. Not
        // without a connection: nothing was flushed, and such a run discards what it holds, as a disconnect() does. Nor
        // once a disconnect() has interrupted the flush: the connection is closed, and a run asked then returned a
        // count, having discarded what it held, where a disconnect() fails it with "Connection is not open".
        $handOvers = $this->drainDelivers ? $this->askDrainParticipants() : [];

        // Deliver the remaining buffered backlog before closing. A handler may await mid-delivery
        // (suspending on ANOTHER fiber, its sid guarded by dispatchingSids with messages still queued)
        // or publish an ack/reply (which now reaches the wire during Draining, #150). Wait - bounded by
        // the single drain deadline computed at entry - for every sid's queue to empty and every
        // in-flight dispatch to finish before releasing state, so a suspended dispatch loop cannot
        // resume into a cleared registry and silently drop its remainder on the lossless path (#149).
        // Each delivery pass is contained so a handler exception is surfaced rather than stranding the
        // connection in Draining - drain() always reaches Closed (#150). The participants' hand-overs are
        // waited for within the same deadline (#207).
        while (true) {
            // A handler that throws, or a full SubscriptionQueue, is reported without cutting the pass short.
            $this->deliverReportingFailures();

            if (!$this->hasUndeliveredDrainBacklog() && !self::anyHandOverPending($handOvers)) {
                break;
            }

            if ($this->monotonicSeconds() >= $drainDeadline) {
                // Deadline reached with backlog still undelivered (a handler suspended past it):
                // releaseRuntimeState() below clears the registry, and the resumed dispatch loop then
                // breaks on the missing subscription and discards the remainder. Make that discard
                // LOUD, never silent (#149 acceptance: either delivered or an error naming the count;
                // mirrors the #123/#134 observable-drop principle). What a participant still holds is
                // discarded the same way from here, before the report counts it, so that a handler that
                // goes on while the report is made or the close below is awaited gets none of it (#207).
                $this->drainDelivers = false;
                $undelivered = $this->countUndeliveredDrainBacklog() + self::countHeldByParticipants($handOvers);
                if ($undelivered > 0) {
                    $this->emitErrorSafely(new ConnectionException(
                        'drain deadline exceeded: ' . $undelivered
                        . ' buffered message(s) were not delivered before close',
                    ));
                }

                break;
            }

            // Yield so a dispatch loop suspended on another fiber can resume and drain its sid's queue;
            // without this the loop would spin while that fiber is never scheduled.
            delay(0.001);
        }
        // The delivery phases are over: from here the close discards what a participant still holds (#207).
        $this->drainDelivers = false;

        // The drain budget ends with the delivery phases above; writePublishFrame()'s Draining branch
        // falls back to its fixed per-publish bound once cleared.
        $this->drainDeadline = null;

        if (!$connected && !$this->closedAnnounced) {
            // The budget ran out before the reconnect finished: the publishes buffered during the
            // outage already reported success, so discarding them must be loud (#123). A reconnect
            // that gave up, or a disconnect() issued meanwhile, closed the connection and announces
            // that close itself: the discard is theirs to report (disconnect() being the documented
            // lossy close).
            $this->reportDiscardedReconnectBuffer('Drain ran out of time waiting for the reconnect');
        }

        // Clear subscription state (also errors out any still-parked pong slots, e.g. this drain's own
        // slot when the flush ended via the deadline).
        $this->releaseRuntimeState();

        // Best-effort like every other terminal path: TransportInterface::close() gives no no-throw
        // guarantee, and drain deliberately routes known-broken sockets here (the dead-socket write
        // failures contained above), so a throwing close() must not rethrow out of drain() and strand
        // the state in Draining - drain() always reaches Closed (#150).
        $this->closeTransportBestEffort();
        $this->state = ConnectionState::Closed;
    }

    /**
     * The connection phase of {@see drain()}: UNSUB every subscription, then flush (PING/PONG) so the
     * deliveries the server already emitted are read - all within the remaining drain budget. Failures
     * are contained so drain() still reaches Closed: a wedged write skips the rest of the phase, and
     * anything else goes to the error listener (#149/#150). A full subscription queue
     * (SlowConsumerPolicy::Error), a handler that throws and an -ERR the server keeps the connection open
     * for are reported without ending the flush: the deliveries still in flight are behind them.
     */
    private function unsubscribeAndFlushForDrain(float $drainDeadline): void
    {
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
            // socket with in-flight MSGs unread, silent loss on the documented lossless path. The
            // writer queues the slot as it writes the PING. A write that wedged past the deadline
            // leaves it queued, since its bytes may still reach the wire when the queue drains (or
            // die with drain()'s socket close), and the epoch teardown clears it; a failed write,
            // whose PING never hit the wire, takes it out again.
            $flushSlot = $this->newPongSlot();
            $this->writeBounded($this->codec->encodePing(), $this->remainingBudgetCancellation($drainDeadline), $flushSlot);

            // Read until the server's PONG for THIS ping confirms the flush (handleFrame completes
            // the slot, or a concurrent teardown errors it - drain()'s close-and-clean-up is right
            // either way), bounded by the REMAINING drain budget (shared with drain()'s backlog wait)
            // so a slow/wedged server cannot hang drain() forever. A partial chunk (0 complete frames
            // yet) must NOT end the flush early - only the PONG or the deadline does. The slot is checked
            // before every read, the first included: another fiber's read (an application's
            // processIncoming() loop, a service's run()) can take the PONG before this loop starts, and
            // a read then waited for that fiber's next one, on a socket with nothing more to come, until
            // the budget ran out.
            $flushCancellation = new TimeoutCancellation(max(0.001, $drainDeadline - $this->monotonicSeconds()));
            try {
                while (!$flushSlot->isComplete() && !$flushCancellation->isRequested()) {
                    // A full subscription queue, a throwing handler or an -ERR the server keeps the connection
                    // open for (its answer to a SUB beyond the maximum subscriptions, say) is reported, not
                    // thrown: ending the flush on it would close the socket on the deliveries still in flight,
                    // lost without a trace. A failure that ends the connection still ends the flush.
                    $read = $this->readChunk(
                        $flushCancellation,
                        \Fiber::getCurrent(),
                        reportOverflows: true,
                        reportHandlerFailures: true,
                        reportFailuresKeepingTheConnection: true,
                        pongSlot: $flushSlot,
                    )->await();

                    if (!$read->consumedBytes) {
                        // Genuinely idle read (empty socket, or another fiber owns the read). Yield so
                        // the event loop advances and the deadline can fire - without this the loop
                        // would busy-spin and starve the timer forever. A read that consumed bytes but
                        // did not complete this drain PONG frame (a large payload arriving in chunks)
                        // loops immediately: the rest is already buffered, so no idle sleep is paid per
                        // partial chunk (#119). A slot completed during any read ends the loop at its head.
                        delay(0.001, cancellation: $flushCancellation);
                    }
                }
            } catch (CancelledException) {
                // Flush deadline reached; close with whatever was delivered.
            }
        } catch (CancelledException) {
            // A drain WRITE wedged past the deadline (backpressure-stalled peer): the socket is
            // unusable, so skip the flush phase - drain()'s teardown closes it, which errors the
            // abandoned write's fiber out. The drain bound now covers the write phase too (#149).
        } catch (\Throwable $flushError) {
            // A failed drain write (dead socket) or a frame's failure that ends the connection (a fatal
            // -ERR) surfaced here. Route it to the error listener (a swallowed failure during the lossless
            // path was invisible before) and return to drain()'s cleanup so it still closes
            // rather than leaving the connection wedged in Draining with the socket open (#150).
            $this->emitErrorSafely($flushError);
        }
    }

    /**
     * The start of a {@see drain()} that finds a reconnect in flight: waits for it within the drain
     * budget and returns whether the connection is open to be drained. It is not when the budget ran out
     * first, when the reconnect gave up, or when the user disconnect()ed meanwhile.
     *
     * A drain that cannot wait - waiting disabled, or called from a listener that runs inside the
     * reconnect (a Disconnected or error listener) and would wait on itself (#145) - closes the connection
     * and throws, like nats.go's Drain() while reconnecting: otherwise the reconnect would reopen a
     * connection the application is shutting down.
     *
     * @param ?\Fiber<mixed, mixed, mixed, mixed> $caller The fiber that called drain().
     */
    private function awaitReconnectForDrain(float $drainDeadline, ?\Fiber $caller): bool
    {
        if (!$this->options->waitForReconnect || ($caller !== null && $caller === $this->recoveryFiber)) {
            $this->closeInsteadOfDrain();

            throw new ConnectionException('Cannot drain while reconnecting: the connection was closed instead');
        }

        try {
            $this->awaitOpenConnection($this->remainingBudgetCancellation($drainDeadline), $caller);
        } catch (\Throwable) {
            // The budget ran out, or the reconnect ended without reopening the connection - however it
            // ended: the drain still winds down and closes.
        }

        // Also when the budget ran out: the connection can come back in the same tick the budget runs out,
        // and must then still be drained (UNSUB, flush), not just closed.
        return $this->state === ConnectionState::Open;
    }

    /**
     * Closes the connection for a {@see drain()} that cannot wait for an in-flight reconnect: sets
     * close-intent so the reconnect stops at its next check, tears down like {@see disconnect()}, and
     * reports what it discards - the buffered publishes and the messages received but not yet delivered.
     * drain() announces the close.
     */
    private function closeInsteadOfDrain(): void
    {
        $this->setCloseIntent();
        $this->closeTransportBestEffort();
        $this->state = ConnectionState::Closed;
        // Reported after the close: no publish can be buffered once close-intent is set, and the
        // reconnect may still have written part of the buffer meanwhile.
        $this->reportDiscardedReconnectBuffer('Drain could not wait for the reconnect');
        $this->reportDiscardedInboundBacklog();
        $this->releaseRuntimeState();
    }

    /**
     * Reports, through the error listener, the publishes still in the reconnect buffer that a close is
     * about to discard: each already reported success to its caller, so dropping them must be loud,
     * like the reconnect-exhausted path (#123).
     */
    private function reportDiscardedReconnectBuffer(string $context): void
    {
        if ($this->reconnectBuffer === '') {
            return;
        }

        $this->emitErrorSafely(new NatsException(sprintf(
            '%s: %d bytes of buffered publishes were discarded',
            $context,
            strlen($this->reconnectBuffer),
        )));
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
     * Asks each drain participant registered now to hand over what it holds ({@see DrainParticipant::handOver()}),
     * for a {@see drain()} whose flush is done (#207). Asking does not suspend: a participant only records the request
     * and wakes what waits on its behalf.
     *
     * @return list<array{DrainParticipant, Future<null>}> Each participant asked, with the future of its hand-over.
     */
    private function askDrainParticipants(): array
    {
        $handOvers = [];
        foreach ($this->drainParticipants as $participant) {
            $handOvers[] = [$participant, $participant->handOver()];
        }

        return $handOvers;
    }

    /**
     * Whether a hand-over {@see askDrainParticipants()} asked for is still under way (#207).
     *
     * @param list<array{DrainParticipant, Future<null>}> $handOvers
     */
    private static function anyHandOverPending(array $handOvers): bool
    {
        foreach ($handOvers as [, $handOver]) {
            if (!$handOver->isComplete()) {
                return true;
            }
        }

        return false;
    }

    /**
     * What the participants whose hand-over is still under way hold and have not handed to a handler: for the "drain
     * deadline exceeded" report, which names it with the backlog the deadline leaves (#207). A hand-over that is over
     * holds nothing the drain could still deliver.
     *
     * @param list<array{DrainParticipant, Future<null>}> $handOvers
     */
    private static function countHeldByParticipants(array $handOvers): int
    {
        $held = 0;
        foreach ($handOvers as [$participant, $handOver]) {
            if (!$handOver->isComplete()) {
                $held += $participant->undelivered();
            }
        }

        return $held;
    }

    /**
     * Reports an asynchronous error through the listener/logger, swallowing a throw from a
     * user-supplied logger/listener: a throwing logger could otherwise re-open the very escape a
     * containment closes (#150). The logger is guarded on its own, so it cannot keep the error from the
     * listener either - for an overflow that is reported instead of thrown, or a message a drop policy
     * discarded, that would hide it altogether. Every report goes through here ({@see emitError()}
     * included), so a secondary/handler error is surfaced WITHOUT masking a primary exception already in
     * flight (drain teardown, and the per-chunk dispatch/drain containment in processIncoming/dispatchFrames,
     * #158). An overflow logs at error level: under SlowConsumerPolicy::Error the application asked to
     * treat a dropped message as an error (the drop policies' reports pass 'debug'). It is reported at
     * most once, whichever path reports it ({@see $reportedOverflows}).
     */
    private function emitErrorSafely(\Throwable $error, string $logLevel = 'error'): void
    {
        if ($error instanceof SlowConsumerException) {
            if (isset($this->reportedOverflows[$error])) {
                return;
            }

            $this->reportedOverflows[$error] = true;
        }

        try {
            $this->logError($error, $logLevel);
        } catch (\Throwable) {
            // A throwing user logger must never break connection handling, nor keep the error from the
            // listener below.
        }

        $this->notifyErrorListener($error);
    }

    /**
     * Publishes payload bytes to the given subject.
     *
     * At-least-once, nats.go-parity semantics while reconnecting: a publish issued while a reconnect is
     * in flight is normally buffered and reports success IMMEDIATELY - the returned Future resolves as
     * soon as the frame is queued, before it has reached any server (after one event-loop tick, which
     * lets a synchronous publisher drive the reconnect - {@see NatsOptions::$waitForReconnect}). If the
     * reconnect then succeeds the buffered frames are flushed in publish order; if it exhausts every
     * attempt, those frames are DISCARDED and this reported success is retroactively void. That
     * exhaustion loss is signalled only out-of-band - via the connection-level
     * {@see ConnectionEvent::Closed} event and the "Reconnect exhausted: N bytes ... discarded" async
     * error (#123) - never through this Future. A caller needing end-to-end delivery confirmation must
     * use a JetStream publish-with-ack rather than treating this success as proof the bytes were
     * transmitted.
     *
     * A publish issued on an Open connection writes directly. If that write fails, it runs one
     * recovery inline and retries afterward, including with waitForReconnect=false. publish() has
     * no timeout or caller cancellation; requests use their own bounded write path.
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
            $frame = $this->encodePublishFrame($subject, $payload, null, $replyTo);

            $this->writePublishFrame($frame);
            $this->recordOutbound($payload);
        });
    }

    /**
     * Publishes payload bytes with NATS headers to the given subject. A header value may be a single
     * string or a list of strings for multi-value (multimap) headers (ADR-4).
     * Delivery and recovery follow the same rules as {@see publish()}.
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
            $frame = $this->encodePublishFrame($subject, $payload, $headers, $replyTo);

            $this->writePublishFrame($frame);
            $this->recordOutbound($payload);
        });
    }

    /**
     * Validates and encodes one PUB/HPUB for both ordinary publishes and bounded request writes.
     * The frame is built once and reused for a retry.
     *
     * @param array<string,string|list<string>>|null $headers Null selects PUB; even an empty array selects HPUB.
     */
    private function encodePublishFrame(string $subject, string $payload, ?array $headers, ?string $replyTo): string
    {
        $this->validatePublishSubjects($subject, $replyTo);
        if ($headers === null) {
            $this->enforceMaxPayload(strlen($payload));

            return $this->codec->encodePublish($subject, $payload, $replyTo);
        }

        // Without the advertised capability, HPUB closes the server connection (#132).
        if ($this->serverInfo?->headersSupported === false) {
            throw new ConnectionException('Server does not advertise headers support; cannot publish with headers (HPUB)');
        }

        // Build and CR/LF-validate once, for both sizing and encoding.
        $headerBlock = NatsHeaders::toWireBlock($headers);
        $this->enforceMaxPayload(strlen($headerBlock) + strlen($payload));

        return $this->codec->encodeHeaderPublishBlock($subject, $payload, $headerBlock, $replyTo);
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
     * Validates a publish subject (cached) and its optional reply subject. The reply is validated
     * uncached: a publish replyTo is typically a per-request unique inbox, so caching it would only
     * churn the memo (see validateSubjectCached()). Shared by ordinary publishes and requests.
     */
    private function validatePublishSubjects(string $subject, ?string $replyTo): void
    {
        $this->validateSubjectCached($subject);
        if ($replyTo !== null) {
            $this->validateSubject($replyTo);
        }
    }

    /**
     * Writes an already-encoded publish frame with the state-appropriate delivery:
     *   - Open: write to the socket, with a single recover-and-retry on a transient write failure. The
     *     retry is sent the way a publish issued after the recovery would be, by the state it finds.
     *   - Draining: write straight to the still-live socket. A draining connection keeps its socket
     *     open until drain() closes it, and a handler's ack/reply (a JetStream ack, respond(), a
     *     request reply) MUST reach the wire - nats.go drains by publishing then closing. Buffering
     *     is wrong (no reconnect will flush it) and refusing would redeliver the just-acked message;
     *     recovery is not attempted (drain is tearing down). #150
     *   - otherwise: buffer while a reconnect is in flight (flushed on reconnect) and yield one
     *     event-loop tick so a synchronous publisher drives that reconnect, else fail loudly - a
     *     publish after the connection has Closed still throws (#146).
     */
    private function writePublishFrame(string $frame): void
    {
        if ($this->state === ConnectionState::Open) {
            $generation = $this->connectionGeneration;
            try {
                $this->transport->write($frame)->await();
            } catch (\Throwable $writeError) {
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
                $this->recoverConnection(failedGeneration: $generation, cause: $writeError);

                // The connection may have moved on while the reconnect announced it: the listener, or what it
                // called, closed it or started another reconnect, or a drain() that waited for the reconnect began
                // meanwhile ({@see recoverConnection()}). Sent as a publish issued now would be, the frame is then
                // buffered behind that reconnect, or written to the connection being drained. Written directly, it
                // would go into a socket already closed, or reach a new one ahead of its CONNECT, which a server
                // that requires authentication answers by closing the connection.
                if ($this->state !== ConnectionState::Open) {
                    // Closed, or being closed other than by a drain() still flushing (which takes the frame, as it
                    // takes any publish): nothing will send the frame. The write's own error says why, as an error
                    // of this library - a built-in transport's raw stream error is wrapped.
                    if (
                        $this->state === ConnectionState::Closed
                        || ($this->closing && $this->state !== ConnectionState::Draining)
                    ) {
                        throw $writeError instanceof NatsThrowable
                            ? $writeError
                            : new TransportClosedException('Transport is not connected', 0, $writeError);
                    }

                    $this->writePublishFrame($frame);

                    return;
                }

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

        $this->yieldToInFlightRecovery();
    }

    /**
     * Buffers an encoded publish while a reconnect is in flight (flushed on reconnect). Returns false
     * when buffering does not apply - no active reconnect, buffering disabled, the connection closing
     * or closed, or the buffer is full.
     */
    private function bufferFrame(string $frame): bool
    {
        if ($this->reconnecting === null || $this->options->reconnectBufferSize <= 0) {
            return false;
        }

        // A terminal path can flip to Closed and then suspend in its transport-close await while
        // $reconnecting is still set; accepting bytes there would report success for a publish
        // that releaseRuntimeState() is about to discard with no error signal (#146). The same holds
        // once close-intent is set (disconnect(), drain()): the reconnect is stopping, so nothing will
        // flush the buffer, and bytes accepted then would otherwise outlive the close and go out on a
        // later connection. Refusing keeps the failure loud and immediate on every terminal path
        // (#123 invariant).
        if ($this->closing || $this->state === ConnectionState::Closed) {
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
     * Whether the application has closed the connection, or is closing it: disconnect() or drain() was called since the
     * last connect(). The pull consumer engine asks it to tell whether an infinite run goes on past a frame that ended
     * the connection ({@see \IDCT\NATS\JetStream\JetStreamContext::consumePipelined()}, #210): any close, a drain()
     * still Draining included, ends the run instead. Whether what the run holds is then handed over or discarded is not
     * this answer's to say but {@see isDiscardingUndelivered()}'s (#207): a drain() in its delivery phase hands it over.
     *
     * @internal For the pull consumer engine (#197, #210); not part of the supported API.
     */
    public function isCloseRequested(): bool
    {
        return $this->closing;
    }

    /**
     * Whether a close of the application's owns what the connection has received and not handed to a handler, so that
     * deliveries leave it alone: the application closed the connection, or is closing it, other than by a drain() still
     * in its delivery phase. A disconnect() discards it (nats.go Close() parity), and so does a drain() with what it
     * cannot deliver itself: from its deadline report on, once its budget has run out, or all of it when it found no
     * connection to drain. A drain() in its delivery phase, the connection Draining, lets deliveries go on, and the
     * handlers' acks out. The pull consumer engine asks it before each message it hands to the handler, wherever the
     * run is, so that what a run's pulls hold follows the rule of the connection's own queues
     * ({@see leftoversBelongToAClose()}, #207): the retire phase, the hand-over before a run fails (#197), the hand-over
     * of an infinite run going on past a frame that ended the connection (#210) and the hand-over a drain() asks for
     * ({@see DrainParticipant}). A disconnect() under way or done, from the handler or from another fiber, then leaves
     * the rest undelivered and unacked, rather than run the handler after it has returned with every ack failing.
     *
     * The state alone cannot tell a drain() in its delivery phase ({@see $drainDelivers}): the drain's teardown, from
     * its deadline report on, and a disconnect() that interrupts a drain both await the transport's close with the state
     * still Draining, and a handler that went on meanwhile got what the drain had just reported as discarded, or what
     * the disconnect() was discarding.
     *
     * @internal For the pull consumer engine (#207); not part of the supported API.
     */
    public function isDiscardingUndelivered(): bool
    {
        return $this->closing && !($this->state === ConnectionState::Draining && $this->drainDelivers);
    }

    /**
     * Registers what a {@see drain()} waits for besides the connection's own backlog ({@see DrainParticipant}) and
     * returns the id {@see removeDrainParticipant()} takes. The pull consumer engine registers each run once its inbox
     * is subscribed (#207): a drain that has flushed asks it to hand over what its pulls hold, and waits for that.
     *
     * @internal For the pull consumer engine (#207); not part of the supported API.
     */
    public function addDrainParticipant(DrainParticipant $participant): int
    {
        $id = ++$this->lastDrainParticipantId;
        $this->drainParticipants[$id] = $participant;

        return $id;
    }

    /**
     * Removes a participant {@see addDrainParticipant()} registered: a drain that has not asked it yet will not. A
     * drain that has already asked it waits for the future it was given, which the participant completes as it goes, so
     * a drain never waits for a participant that has gone (#207). An id no longer registered is ignored.
     *
     * @internal For the pull consumer engine (#207); not part of the supported API.
     */
    public function removeDrainParticipant(int $id): void
    {
        unset($this->drainParticipants[$id]);
    }

    /**
     * Registers a subscription callback and sends a SUB command.
     *
     * While a reconnect is in flight it first waits for it, within the request timeout
     * ({@see NatsOptions::$waitForReconnect}), and then subscribes on the new connection.
     *
     * @param callable(NatsMessage):void $handler
     * @return Future<int>
     */
    public function subscribe(string $subject, callable $handler, ?string $queue = null): Future
    {
        $caller = \Fiber::getCurrent();

        return async(fn(): int => $this->subscribeInternal($subject, $handler, $queue, $caller));
    }

    /**
     * Body of {@see subscribe()}, also used for the mux reply inbox and the guarded inboxes ({@see subscribeGuarded()}).
     *
     * @param callable(NatsMessage):void $handler
     * @param ?\Fiber<mixed, mixed, mixed, mixed> $caller The fiber that issued the subscribe.
     * @param (\Closure(int): void)|null $registered Called with the sid once the subscription is registered, before
     *        its SUB is written, so that whichever fiber reads the server's answer to the SUB finds what it records.
     * @param string $before Frames written ahead of the SUB, in the same write.
     * @param string $after Frames written right behind the SUB, in the same write.
     */
    private function subscribeInternal(
        string $subject,
        callable $handler,
        ?string $queue,
        ?\Fiber $caller,
        ?\Closure $registered = null,
        string $before = '',
        string $after = '',
    ): int {
        if ($this->state !== ConnectionState::Open) {
            try {
                $this->awaitOpenConnection(new TimeoutCancellation($this->options->requestTimeoutMs / 1000), $caller);
            } catch (CancelledException) {
                throw new TimeoutException(sprintf('Subscribe to "%s" timed out waiting for the connection to be re-established', $subject));
            }
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
        if ($registered !== null) {
            $registered($sid);
        }

        $generation = $this->connectionGeneration;
        try {
            $this->transport->write($before . $this->codec->encodeSubscribe($subject, $sid, $queue) . $after)->await();
        } catch (\Throwable $writeError) {
            // The socket is dead. The subscription stays registered while the connection recovers, so the
            // reconnect subscribes it on the new connection; it is rolled back only when the connection does
            // not come back in time, so that the registry does not keep an entry whose sid the caller never
            // learns (#116).
            try {
                $this->recoverAfterFailedWrite($generation, $writeError, new TimeoutCancellation($this->options->requestTimeoutMs / 1000), $caller);
            } catch (CancelledException) {
                $this->dropSubscriptionState($sid);

                throw new TimeoutException(sprintf('Subscribe to "%s" timed out waiting for the connection to be re-established', $subject));
            } catch (\Throwable $recoveryError) {
                $this->dropSubscriptionState($sid);

                throw $recoveryError;
            }

            // A terminal close released it, and a connect() opened a fresh connection since: nothing
            // subscribed it there. (The reply inbox's and a guarded inbox's registration also go when the
            // server rejects their replayed SUB; ensureMuxInbox() and subscribeGuarded() report that instead.)
            if (!isset($this->subscriptionMeta[$sid])) {
                throw new ConnectionException(sprintf('Subscribe to "%s" failed: the connection was closed', $subject), 0, $writeError);
            }
        }

        return $sid;
    }

    /**
     * Subscribes a reply inbox whose rejection by the connection's subscription limit must reach the operation
     * waiting on it, whichever fiber's read meets the -ERR: the JetStream pull fetch, the pull pipeline and the
     * batched Direct Get inboxes (#175). It is the reply inbox's rule ({@see ensureMuxInbox()}, {@see $muxFence}),
     * generalised: a PING is written right behind the SUB, in the same write, and the sid is unconfirmed
     * ({@see $unconfirmedSids}) until the PONG answering that PING or a delivery on it, both of which prove the
     * server holds it. The server answers a SUB it rejects with 'maximum subscriptions exceeded' ahead of that
     * PONG, and the -ERR names no subject, so one read before the sid is confirmed is taken as the SUB's: the
     * subscription is dropped, an UNSUB is owed for it ({@see $guardedSidsToRelease}, since the -ERR may have
     * been another SUB's after all), and $onRejected is called with the -ERR text, in the dispatch of the -ERR,
     * before any read it ended returns ({@see rejectUnconfirmedSubscriptions()}). The -ERR then fails the read that
     * met it, or is reported, as it always did. $onRejected is also the sid's rejection handler for a permissions
     * violation naming its subject ({@see markSubscriptionRejectionHandler()}), which leaves the subscription for
     * the operation to unsubscribe, as before.
     *
     * Several SUBs unconfirmed at once - two fetches, a fetch and the reply inbox - cannot be told apart by an -ERR
     * that names none: each is treated as rejected and each operation fails, as the reply inbox is dropped. The
     * window is one round trip, plus the time the SUB write waits behind earlier writes. A reconnect replays the
     * subscription with a PING behind it again ({@see resubscribeAll()}), with the same consequence. The
     * registration is recorded before the SUB is written, so whichever fiber reads the server's answer finds it.
     *
     * The subscribe part behaves as {@see subscribe()}: it waits for a reconnect in flight, within the request
     * timeout, and a failed write leaves the registration for the reconnect to replay. When the new server rejects
     * that replay, the replay's drain drops the registration, $onRejected called, and this reports the rejection,
     * with a ConnectionException naming the subject and the limit, where {@see subscribeInternal()} sees a closed
     * connection - as {@see ensureMuxInbox()} reports a dropped reply inbox; the replay writes the UNSUB
     * ({@see finishReplayWindow()}). Otherwise the caller unsubscribes the sid when its operation ends, whether or
     * not it was rejected: {@see unsubscribe()} then writes the UNSUB a rejection owes.
     *
     * @internal Low-level mechanism for the JetStream reply inboxes; not general API.
     *
     * @param callable(NatsMessage):void $handler
     * @param \Closure(string): void $onRejected Called with the -ERR text once the server may have rejected, or has
     *        rejected, the subscription; at most once for the limit. It must not suspend: it runs inside the
     *        dispatch of the -ERR.
     * @return Future<int>
     */
    public function subscribeGuarded(string $subject, callable $handler, \Closure $onRejected): Future
    {
        $caller = \Fiber::getCurrent();

        return async(function () use ($subject, $handler, $onRejected, $caller): int {
            // Recorded here as well, so that a rejection read while this fiber waits in its subscribe - by the
            // drain of the reconnect a failed write started - is told from a connection that closed.
            /** @var string|null $rejection */
            $rejection = null;
            $recordingRejection = static function (string $error) use ($onRejected, &$rejection): void {
                $rejection ??= $error;
                $onRejected($error);
            };

            try {
                return $this->subscribeInternal(
                    $subject,
                    $handler,
                    null,
                    $caller,
                    registered: function (int $sid) use ($recordingRejection): void {
                        $this->guardedSids[$sid] = true;
                        $this->subscriptionRejectionHandlers[$sid] = $recordingRejection;
                        // Queued in the step that hands its PING to the transport, as every PING's slot is, so that
                        // whatever PINGs other fibers write, this slot completes on the PONG answering that PING.
                        $this->unconfirmedSids[$sid] = $this->enqueuePongSlot();
                    },
                    after: $this->codec->encodePing(),
                );
            } catch (ConnectionException $e) {
                throw $this->guardedSubscribeFailure($subject, $rejection, $e);
            }
        });
    }

    /**
     * The error a guarded subscribe fails with ({@see subscribeGuarded()}): $failure itself, unless the server
     * rejected the subscription while the subscribe waited - the SUB write failed, the reconnect replayed the SUB,
     * and the new server rejected it, which dropped the registration - and the connection is open: then
     * {@see subscribeInternal()} reports a closed connection, and this says what happened instead, as
     * {@see ensureMuxInbox()} does for the reply inbox, with $failure as the cause.
     *
     * @param string|null $rejection The -ERR text the rejection handler recorded, or null while it recorded none.
     */
    private function guardedSubscribeFailure(string $subject, ?string $rejection, ConnectionException $failure): ConnectionException
    {
        if ($rejection === null || $this->state !== ConnectionState::Open) {
            return $failure;
        }

        if ($this->isSubscriptionLimitError($rejection)) {
            return new ConnectionException(
                sprintf(
                    'Subscribe to "%s" failed: the server may have rejected it because the connection is at its '
                    . 'subscription limit (maximum subscriptions exceeded)',
                    $subject,
                ),
                0,
                $failure,
            );
        }

        return new ConnectionException(sprintf('Subscribe to "%s" failed: the server rejected it: %s', $subject, $rejection), 0, $failure);
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
     * A sid that is no longer registered is left alone: another fiber's read can have dropped it between the
     * SUB write and this call, when the server rejected it ({@see subscribeGuarded()}), and nothing is queued
     * for a sid that is gone.
     *
     * @internal Low-level mechanism for the JetStream request/pull inboxes; not general API.
     */
    public function markSubscriptionUnbounded(int $sid): void
    {
        if (!isset($this->subscriptions[$sid])) {
            return;
        }

        $this->unboundedSids[$sid] = true;
    }

    /**
     * Registers a callback invoked (with the raw -ERR text) when the server rejects this sid's
     * subscription with an async permissions violation. See {@see $subscriptionRejectionHandlers}.
     * A sid that is no longer registered is left alone, as by {@see markSubscriptionUnbounded()}.
     *
     * @internal Low-level mechanism for long-lived JetStream reply inboxes; not general API.
     */
    public function markSubscriptionRejectionHandler(int $sid, \Closure $handler): void
    {
        if (!isset($this->subscriptions[$sid])) {
            return;
        }

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
     * For a guarded inbox the client treated as rejected ({@see subscribeGuarded()}), already dropped, this
     * writes the UNSUB the rejection owes, on an open connection ({@see releaseRejectedSubscription()}).
     *
     * @return Future<void>
     */
    public function unsubscribe(int $sid, ?int $maxMessages = null): Future
    {
        return async(function () use ($sid, $maxMessages): void {
            if (!isset($this->subscriptionMeta[$sid])) {
                $this->releaseRejectedSubscription($sid);

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
                    // On a broken connection the server cannot be told now; recovery re-arms it.
                    try {
                        $this->transport->write($this->codec->encodeUnsubscribe($sid, $maxMessages))->await();
                    } catch (\Throwable) {
                        // The socket is dead: the arm stays recorded for the recovery that the next operation
                        // needing the socket starts, which arms it on the new connection. Not thrown, as on a
                        // connection that is not open.
                    }
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
            } catch (\Throwable) {
                // The socket is dead, and the server dropped the subscription with the connection, so this
                // unsubscribe has what it asked for. Not thrown, exactly as on a connection that is not open:
                // unsubscribe() runs in finally-based clean-up, where an error would mask the caller's own
                // (#116). The next operation that needs the socket recovers the connection.
            }

            // Dropped even when the write failed, so that recovery does not subscribe the sid again (#116).
            $this->dropSubscriptionState($sid);
        });
    }

    /**
     * Drains a single subscription: sends UNSUB so the server stops delivering, flushes so any
     * messages already in flight are received and dispatched to the handler, then removes the local
     * subscription state. Mirrors nats.go / nats.java per-subscription `Drain()` (#43).
     *
     * One budget (~requestTimeoutMs) bounds the wait for a reconnect, the UNSUB and the flush. While a
     * reconnect is in flight it first waits for it, like flush() ({@see NatsOptions::$waitForReconnect}),
     * and then drains on the new connection: the reconnect never re-subscribes a sid being drained, but
     * one that had already re-subscribed it keeps it until just before it goes live, and the server can
     * deliver on it until then. When it cannot wait - waiting disabled, a caller inside the reconnect
     * (a Disconnected or error listener), or the budget ran out - it delivers the messages already received
     * and removes the subscription; should the reconnect already have re-subscribed the sid, messages
     * the server sends on it before the reconnect's UNSUB lands are then dropped. During a drain() the
     * drain takes the subscription over: drain() has already unsubscribed it, its flush may still bring
     * messages for it, and it delivers those, and what is queued, before it removes every subscription.
     *
     * A failed UNSUB or flush and a handler that throws are reported through the error listener: like
     * drain(), it does not reject for them, and a throwing handler does not cost the messages behind it.
     * Nor does another subscription's full queue (SlowConsumerPolicy::Error), or an -ERR the server keeps
     * the connection open for: either is reported and the flush reads on to its PONG. A connection lost
     * during the flush (when the server closes the connection right after such an -ERR, say) is recovered
     * by the flush's own read, as by any read that is first to notice it; the reconnect runs on in a fiber
     * of its own, and the flush fails as soon as its first attempt ends the lost connection's PONGs, since
     * the one it waited for died with the socket (#178). That failure is reported, and the subscription is
     * removed; the reconnect never re-subscribes it. The call used to last until the reconnect ended.
     *
     * When a delivery for the sid is already under way - on another fiber, or it is the handler that
     * called this, draining its own subscription - that delivery hands over the messages queued behind
     * the current one and then removes the subscription, and this resolves without waiting for it
     * (waiting could mean waiting on itself). A call for a sid that is unknown, already removed or
     * already being drained resolves at once, as does one made during a drain().
     *
     * @return Future<void>
     */
    public function drainSubscription(int $sid): Future
    {
        $caller = \Fiber::getCurrent();

        return async(function () use ($sid, $caller): void {
            if (!isset($this->subscriptionMeta[$sid])) {
                return;
            }

            // From here on no reconnect may re-subscribe this sid: the server would go on delivering to a
            // sid nothing handles any more - for a queue group, taking a share of the group's messages.
            // The handler and the queue stay until the end, so the flush below still delivers.
            unset($this->subscriptionMeta[$sid]);

            // One budget for the whole call, like flush(): the reconnect wait, the UNSUB and the flush.
            $deadline = $this->monotonicSeconds() + max(0.1, $this->options->requestTimeoutMs / 1000);

            if ($this->state !== ConnectionState::Open && $this->state !== ConnectionState::Draining) {
                try {
                    // A reconnect that already re-subscribed the sid unsubscribes it only just before it
                    // goes live, and the server can deliver on it until then: draining on the new
                    // connection delivers those messages too.
                    $this->awaitOpenConnection($this->remainingBudgetCancellation($deadline), $caller);
                } catch (\Throwable) {
                    // Nothing to wait for, waiting disabled or impossible, the budget ran out, or the
                    // reconnect gave up: what already arrived is delivered below.
                }
            }

            if ($this->state === ConnectionState::Open) {
                try {
                    // Stop new deliveries for this sid, then flush so in-flight messages are received...
                    // The UNSUB's wait is bounded like drain()'s writes (#149): a peer that stopped reading
                    // must not hold this call past its budget.
                    try {
                        $this->writeBounded($this->codec->encodeUnsubscribe($sid), $this->remainingBudgetCancellation($deadline));
                    } catch (CancelledException) {
                        throw new TimeoutException('drainSubscription timed out writing UNSUB (transport backpressure)');
                    }

                    // Neither a full queue, a throwing handler nor an -ERR the server keeps the connection open
                    // for ends the flush early: the messages still in flight for this subscription would arrive
                    // after it is removed, and be dropped.
                    $this->flushWithin(
                        $deadline,
                        $caller,
                        reportOverflows: true,
                        reportHandlerFailures: true,
                        reportFailuresKeepingTheConnection: true,
                    );
                } catch (\Throwable $flushError) {
                    // A failed UNSUB or flush (a dead socket, a timeout, a frame's failure that ends the
                    // connection) is reported the way drain() reports it (#150). Removing the subscription
                    // below is still safe: a lost connection takes the server-side one with it.
                    $this->emitErrorSafely($flushError);
                }
            }

            if ($this->state === ConnectionState::Draining) {
                // drain() has unsubscribed every subscription, and its flush may still bring messages for
                // this one: removing it now would drop them. The drain delivers them, and what is queued,
                // within its budget, reports what it cannot deliver, and then removes every subscription.
                return;
            }

            if (isset($this->dispatchingSids[$sid])) {
                // A delivery for this sid is under way - on another fiber, or it is the very handler that
                // called this. Waiting for it could mean waiting on itself, and removing the subscription
                // now would drop what is queued behind the current message: hand both over to that
                // delivery instead (drainPendingForSid() removes the subscription once its queue is empty).
                $this->removeAfterDelivery[$sid] = true;

                return;
            }

            // ...deliver whatever arrived for it. A handler that throws is reported and delivery goes on
            // with the next message, as in drain() (#150); past the budget of a drain() that winds down
            // without a connection it ends there instead, like that drain's own delivery.
            while (true) {
                try {
                    $this->drainPendingForSid($sid);

                    break;
                } catch (\Throwable $handlerError) {
                    $this->emitErrorSafely($handlerError);
                    if ($this->drainDeadline !== null && $this->monotonicSeconds() >= $this->drainDeadline) {
                        break;
                    }
                }
            }

            // ...then remove the handler and local state, reporting what could not be delivered.
            $undelivered = isset($this->pendingMessages[$sid]) ? $this->pendingMessages[$sid]->count() : 0;
            if ($undelivered > 0) {
                $this->emitErrorSafely(new NatsException(sprintf(
                    'drainSubscription: %d buffered message(s) for sid %d were discarded undelivered',
                    $undelivered,
                    $sid,
                )));
            }

            $this->dropSubscriptionState($sid);
        });
    }

    /**
     * Flushes the outbound buffer and waits for the server to round-trip a PONG, confirming the server
     * has processed everything written so far. Useful to ensure a SUBSCRIBE is registered server-side
     * before relying on it (e.g. before publishing a request to a freshly-subscribed responder).
     * Bounded by the configured request timeout: one budget covers the whole flush. While a reconnect
     * is in flight it first waits for it within that budget ({@see NatsOptions::$waitForReconnect}).
     *
     * @return Future<void>
     */
    public function flush(): Future
    {
        $caller = \Fiber::getCurrent();

        return async(function () use ($caller): void {
            $this->flushWithin(
                $this->monotonicSeconds() + max(0.1, $this->options->requestTimeoutMs / 1000),
                $caller,
                reportOverflows: !$this->options->slowConsumerErrorsFailOperations,
                reportHandlerFailures: !$this->options->handlerErrorsFailOperations,
            );
        });
    }

    /**
     * {@see flush()} for a caller that would only swallow what the flush's reads meet: a full subscription
     * queue, a handler that throws while they deliver and an -ERR the server keeps the connection open for are
     * reported through the error listener, and the flush reads on to its PONG, as drainSubscription()'s does.
     * A failure that ends the connection, and the timeout, still fail it.
     *
     * @internal For the service framework's drain(); not part of the supported API.
     *
     * @return Future<void>
     */
    public function flushReportingFailures(): Future
    {
        $caller = \Fiber::getCurrent();

        return async(function () use ($caller): void {
            $this->flushWithin(
                $this->monotonicSeconds() + max(0.1, $this->options->requestTimeoutMs / 1000),
                $caller,
                reportOverflows: true,
                reportHandlerFailures: true,
                reportFailuresKeepingTheConnection: true,
            );
        });
    }

    /**
     * Body of {@see flush()}, {@see flushReportingFailures()} and {@see rtt()}, and the flush of
     * {@see drainSubscription()}. One monotonic deadline bounds every phase - the wait for an in-flight
     * reconnect, the PING write and the PONG wait - so the flush honours the single request-timeout bound it
     * documents (the write and read phases used to get a full budget each).
     *
     * @param ?\Fiber<mixed, mixed, mixed, mixed> $caller The fiber that called flush()/rtt().
     * @param bool $reportOverflows Report a full queue of a subscription that one of the flush's reads runs
     *        into ({@see SlowConsumerException}) and read on, instead of ending the flush with it: for
     *        flush() and rtt(), which wait for a PONG of their own ({@see readIncomingForOperation()}), and
     *        for drainSubscription(), which needs the messages still in flight before the PONG.
     * @param bool $reportHandlerFailures Report a handler that throws while the flush delivers, and read on,
     *        instead of ending the flush with it: for flush() and rtt(), as for any operation that waits for a
     *        result of its own, unless {@see NatsOptions::$handlerErrorsFailOperations}; for
     *        drainSubscription(), for the same reason as above; and for flushReportingFailures(), whose
     *        caller would only swallow it.
     * @param bool $reportFailuresKeepingTheConnection Report a frame's failure that leaves the connection open,
     *        such as an -ERR the server keeps it open for, and read on, instead of ending the flush with it: for
     *        drainSubscription() and flushReportingFailures(), for the same reasons. flush() and rtt() still fail
     *        with it: the server rejected something their caller sent, and they are to confirm what was sent.
     */
    private function flushWithin(
        float $deadline,
        ?\Fiber $caller,
        bool $reportOverflows,
        bool $reportHandlerFailures = false,
        bool $reportFailuresKeepingTheConnection = false,
    ): void {
        if ($this->state !== ConnectionState::Open) {
            try {
                $this->awaitOpenConnection($this->remainingBudgetCancellation($deadline), $caller);
            } catch (CancelledException) {
                throw new TimeoutException('Flush timed out waiting for the connection to be re-established');
            }
        }

        // FIFO pong correlation (#117): completion of THIS slot means "the server processed
        // everything written before THIS flush's PING". A stale PONG answering an earlier
        // (heartbeat or timed-out) PING completes that PING's slot, never this one, and a
        // concurrent flush timing out cannot release this waiter. The writer queues the slot as
        // it writes the PING, so a PING another fiber writes meanwhile - the one behind the
        // reply inbox's SUB, say - keeps its own PONG.
        $slot = $this->newPongSlot();
        $generation = $this->connectionGeneration;
        try {
            // The WAIT is bounded (the write itself cannot be cancelled): flush() documents a
            // request-timeout bound, but a backpressure-suspended write held it forever before
            // the read phase's deadline could even start (#149's write-phase twin).
            $this->writeBounded($this->codec->encodePing(), $this->remainingBudgetCancellation($deadline), $slot);
        } catch (CancelledException) {
            // Write wedged past the flush budget. The PING may still reach the wire whenever
            // the queue drains, so its slot deliberately STAYS queued (same rule as the
            // read-phase timeout below); epoch teardown clears it if the PONG never comes.
            throw new TimeoutException('Flush timed out writing PING (transport backpressure)');
        } catch (\Throwable $writeError) {
            // The PING never hit the wire, and the writer took its slot out again, so correlation
            // stays aligned with wire order (nats.go removePongFromList parity).

            // The socket is dead. The connection recovers within this flush's budget, and the flush fails
            // anyway, as when its PONG dies with the socket: what it was to confirm went to the dead one.
            try {
                $this->recoverAfterFailedWrite($generation, $writeError, $this->remainingBudgetCancellation($deadline), $caller);
            } catch (CancelledException) {
                throw new TimeoutException('Flush timed out waiting for the connection to be re-established');
            }

            throw new ConnectionException('Connection lost before the server answered the PING', 0, $writeError);
        }

        $cancellation = $this->remainingBudgetCancellation($deadline);
        try {
            while (!$slot->isComplete()) {
                $read = $this->readChunk(
                    $cancellation,
                    \Fiber::getCurrent(),
                    $reportOverflows,
                    reportHandlerFailures: $reportHandlerFailures,
                    reportFailuresKeepingTheConnection: $reportFailuresKeepingTheConnection,
                    pongSlot: $slot,
                )->await();

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
    }

    /**
     * Reads one transport chunk, parses frames, and dispatches message callbacks, reporting BOTH the
     * number of complete frames dispatched and whether the read consumed bytes off the wire (#119).
     *
     * A wait loop uses {@see IncomingChunkResult::$consumedBytes} to tell partial-frame progress (a
     * large payload arriving in socket-sized chunks: bytes consumed, frame not yet complete) apart
     * from a genuinely idle read (empty socket / another fiber owned the read): it loops immediately in
     * the former case and yields 1 ms only in the latter. {@see processIncoming()} exposes the same
     * cycle as a frame count only, preserving its existing contract.
     *
     * When another fiber is reading the socket - a request waiting for its reply, the heartbeat, a
     * flush - this read waits for that one to finish, bounded like the read itself by $cancellation, and
     * returns without reading (no frames, no bytes consumed): that read delivered what it read. A loop
     * of these calls therefore always hands the event loop control, so timers and socket reads keep
     * running.
     *
     * While a reconnect is in flight the read first waits for it ({@see NatsOptions::$waitForReconnect}),
     * bounded like the read itself by $cancellation, and then reads from the new connection. A read that
     * fails while another fiber already runs the recovery likewise waits for it only until
     * $cancellation fires.
     *
     * A handler that throws in this read is thrown once the read has delivered the other subscriptions'
     * messages, and its own subscription's delivery stops at the failing message, the rest of it left queued
     * (#177). Before it reads, or waits for another fiber's read, the read continues every subscription whose
     * delivery an earlier read stopped like that, in sid order, so that their later messages are delivered with
     * nothing new on the wire (#186), and then any that a read in one of the handlers it runs, or another fiber's
     * delivery, stops meanwhile. It does so with its own rules: a handler that throws there is thrown once that
     * pass is over, and the read ends without reading; a second subscription that fails in the same pass is
     * reported. Otherwise the read goes on as it would have, waiting on an idle socket as any read does, and its
     * result counts only what it took off the wire, not what it continued: one that continued such a subscription
     * and then waited for another fiber's read returns no frames and no bytes consumed. The cancellation bounds
     * the read's waits, not this delivery: a read whose cancellation has already fired still continues the
     * remainder, then throws CancelledException. The later messages of a failing subscription used to wait for a
     * read that received anything, on an otherwise idle connection the server's next PING.
     *
     * Not continued: a subscription whose delivery is under way further up some fiber's stack, which delivers the
     * rest itself, and what a disconnect() already under way when the read starts is to discard
     * ({@see leftoversBelongToAClose()}); a pass already running when a disconnect() begins goes on to the other
     * stopped subscriptions, as any pass under way does. Nor the later subscriptions of a delivery held up in
     * another fiber's handler that awaits, unless a handler failure had already stopped one of them, which is then
     * continued whole, in order, the messages the held-up delivery brought for it included; the others wait for
     * that delivery, which goes on once the handler returns. A read already waiting on the socket, or for another
     * fiber's read, is not woken when another fiber's read stops a subscription meanwhile: that remainder waits for
     * your next read, or for any read that receives something.
     *
     * A frame that ends the connection - a fatal -ERR, which the server sends right before it closes the
     * socket, or a PONG the socket would not take - recovers it before its error reaches the caller: with
     * reconnect off the connection is then Closed, and with reconnect on the read waits for the reconnect
     * within $cancellation, as it would for one in flight, or not at all with waiting disabled (#171).
     *
     * Under {@see SlowConsumerPolicy::Error}, a full subscription queue this read runs into - whichever
     * subscription's - is thrown as a {@see SlowConsumerException}.
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
        return $this->readChunk($cancellation, \Fiber::getCurrent(), reportOverflows: false, continueStoppedSubscriptions: true);
    }

    /**
     * {@see readIncoming()} for an operation that reads while it waits for a result of its own - a
     * request's reply, a fetch's messages, a polling queue's next message. Under
     * {@see SlowConsumerPolicy::Error}, a full queue of another subscription that the read runs into is
     * reported through the error listener instead of failing the operation: that subscriber fell behind,
     * the operation did not. An overflow of $ownSid still fails it. With
     * {@see NatsOptions::$slowConsumerErrorsFailOperations} every overflow fails it, as before that option.
     * Likewise a handler of another subscription that throws is reported, and the rest of the read is still
     * delivered: that handler failed, the operation did not. A handler of $ownSid that throws still fails it,
     * and with {@see NatsOptions::$handlerErrorsFailOperations} every handler does, as before that option, the
     * other subscriptions' messages behind it still delivered first ({@see deliverPending()}). An -ERR the
     * server keeps the connection open for still fails the operation: it names no subscription, and it is often
     * the answer to what the operation itself sent, its SUB or its PUB.
     *
     * With $ownSid the read first takes what is already queued for that subscription, before it reads or waits for
     * another fiber's read (#179): an earlier read can have queued the operation's message behind a delivery it has not
     * finished, held up in the handler of a lower sid that awaits, which is the handler the operation itself runs in
     * when a handler polls a SubscriptionQueue, or one awaiting an HTTP or a database call in another fiber. That one
     * subscription is delivered, with the rules below for its overflow and its handler, and the read returns without
     * reading, so that the operation looks again; the other subscriptions' queued messages stay in order for the
     * delivery under way. Not while that subscription's own delivery is under way, which delivers it, and not what a
     * disconnect() under way is to discard ({@see leftoversBelongToAClose()}). An operation used to get its message
     * once the in-order delivery reached it, so a handler ahead of it that outlasted its deadline made it time out.
     *
     * With $ownSid the read also ends, without reading and with nothing consumed, as soon as anything is delivered
     * to that subscription after this call, by whichever fiber's read: the read of another fiber whose delivery was
     * still under way, the one that took the socket during the hops before this read's own fiber started, or the
     * one that recovered a lost connection this read waited for. The read would otherwise wait on the socket, with
     * what the operation waits for already delivered, for the server's next bytes or the operation's deadline
     * (#174). The operation then looks again: call this right after looking for the result, with no await in
     * between, since what is delivered before the call does not end the read.
     *
     * A read with $ownSid, whose subscription still exists, that is the first to notice a lost connection starts the
     * reconnect and waits for it only within $cancellation and that wake-up, as it waits for a reconnect another
     * fiber runs, or with waiting disabled fails at once with "Connection is not open" (#178); a read without a
     * wake-up - {@see readIncoming()}, a serving loop's read, one for a sid since removed - runs the reconnect itself
     * and waits for all of it.
     *
     * @internal For the library's own operations (JetStream, Key/Value, polling queues, services);
     *           applications read with {@see readIncoming()} or {@see processIncoming()}.
     *
     * @param int|null $ownSid The operation's own subscription, whose overflow or failing handler still fails
     *        the operation, and whose next delivery ends the read.
     * @param bool $alwaysReport Report every overflow and every handler that throws while the read delivers,
     *        whatever the options say, the messages behind it still delivered, and an -ERR the server keeps the
     *        connection open for: for a read whose caller would only swallow them, such as a serving loop. Such
     *        a read also delivers what an earlier or a concurrent read has queued and not reached - the later sids
     *        of a delivery held up in another fiber's handler, the rest of a subscription whose handler threw in
     *        the application's read - unless a disconnect() is closing the connection, which discards it. A read
     *        that receives anything during the close still delivers it with what it received, as any read does. A
     *        failure that ends the connection is still thrown, once the connection has recovered.
     * @return Future<IncomingChunkResult>
     *
     * @phpstan-impure Mutates connection state, like readIncoming().
     */
    public function readIncomingForOperation(?Cancellation $cancellation = null, ?int $ownSid = null, bool $alwaysReport = false): Future
    {
        // Taken here, in the operation's fiber, before the read's own fiber starts a few event-loop hops later: a
        // delivery made during those hops ends the read as well.
        return $this->operationRead($cancellation, $ownSid, $alwaysReport, $ownSid === null ? null : $this->nextDeliveryTo($ownSid));
    }

    /**
     * {@see readIncomingForOperation()} with the wake-up given: for request() and requestMany(), whose replies come on
     * the reply inbox every request shares, so that their read ends on their own reply rather than on any delivery to
     * that inbox.
     *
     * @param Cancellation|null $wake Fires once what the operation waits for may have arrived ({@see readChunk()}).
     * @return Future<IncomingChunkResult>
     *
     * @phpstan-impure Mutates connection state, like readIncoming().
     */
    private function operationRead(?Cancellation $cancellation, ?int $ownSid, bool $alwaysReport, ?Cancellation $wake): Future
    {
        return $this->readChunk(
            $cancellation,
            \Fiber::getCurrent(),
            reportOverflows: $alwaysReport || !$this->options->slowConsumerErrorsFailOperations,
            ownSid: $ownSid,
            reportHandlerFailures: $alwaysReport || !$this->options->handlerErrorsFailOperations,
            reportFailuresKeepingTheConnection: $alwaysReport,
            deliverLeftovers: $alwaysReport,
            wake: $wake,
        );
    }

    /**
     * What an operation's read for $sid waits on besides its deadline ({@see readIncomingForOperation()}): a
     * cancellation that the next delivery to $sid fires, whichever fiber's read makes it, and the subscription's
     * removal. Null when $sid has no subscription, since nothing will be delivered to it.
     */
    private function nextDeliveryTo(int $sid): ?Cancellation
    {
        if (!isset($this->subscriptions[$sid])) {
            return null;
        }

        return ($this->deliveryWakes[$sid] ??= new DeferredCancellation())->getCancellation();
    }

    /**
     * Ends the reads of operations waiting for a delivery to $sid ({@see nextDeliveryTo()}): one was just made, or the
     * subscription is gone. The next read for $sid gets a new wake-up.
     */
    private function wakeReadsWaitingFor(int $sid): void
    {
        $wake = $this->deliveryWakes[$sid] ?? null;
        if ($wake === null) {
            return;
        }

        unset($this->deliveryWakes[$sid]);
        $wake->cancel();
    }

    /**
     * The wake-up of a read that waits for a pong slot ({@see readChunk()}): fires once the slot completes, with its PONG
     * or with the end of the connection, whichever fiber completes it. A slot completes once and stays complete, so the
     * wake-up can be taken at any time: one taken after the slot completed fires at once.
     *
     * @param DeferredFuture<null> $slot
     */
    private function wakeOnPong(DeferredFuture $slot): Cancellation
    {
        $this->pongWakes ??= new \WeakMap();
        $known = $this->pongWakes[$slot] ?? null;
        if ($known !== null) {
            return $known;
        }

        $wake = new DeferredCancellation();
        $slot->getFuture()->finally($wake->cancel(...))->ignore();

        return $this->pongWakes[$slot] = $wake->getCancellation();
    }

    /**
     * What a wait of an operation's read waits with ({@see readChunk()}): the caller's cancellation and the read's
     * wake-up, whichever of the two are given.
     */
    private static function waitWith(?Cancellation $cancellation, ?Cancellation $wake): ?Cancellation
    {
        if ($wake === null || $cancellation === null) {
            return $wake ?? $cancellation;
        }

        return new CompositeCancellation($cancellation, $wake);
    }

    /**
     * Whether a wait of an operation's read ended because its wake-up fired ({@see readChunk()}). Not when the caller's
     * own cancellation fired as well: the caller's deadline wins, as before there was a wake-up.
     *
     * @phpstan-impure Reads cancellations that other fibers fire while the read is suspended.
     */
    private static function wokenUp(?Cancellation $wake, ?Cancellation $cancellation): bool
    {
        return ($wake?->isRequested() ?? false) && !($cancellation?->isRequested() ?? false);
    }

    /**
     * The read behind {@see readIncoming()} and {@see readIncomingForOperation()}.
     *
     * @param ?\Fiber<mixed, mixed, mixed, mixed> $caller The fiber that asked for the read.
     * @param bool $reportOverflows Report a full subscription queue the read runs into instead of throwing
     *        it, except one of $ownSid ({@see dispatchFrames()}).
     * @param int|null $ownSid The operation's own subscription ({@see readIncomingForOperation()}). What an earlier
     *        read has queued for it and not reached is delivered first, before the read-slot decision and after a
     *        wait for another fiber's read, next to the leftovers a serving loop's read delivers, and the read then
     *        returns without reading ({@see deliverQueuedForOwnSid()}, #179): that message is behind a delivery
     *        held up in a lower sid's handler, which can be the very handler the operation runs in, and nothing else
     *        would deliver it before that handler returned. Only that one subscription: the others' queued messages
     *        stay in order for the delivery under way.
     * @param bool $reportHandlerFailures Report a handler that throws while the read delivers, and deliver
     *        the rest of its subscription too, instead of throwing its exception once the other subscriptions'
     *        messages are delivered ({@see deliverPending()}), unless it is a handler of $ownSid: for a drain,
     *        for an operation's read, which waits for a result of its own ({@see readIncomingForOperation()}),
     *        and for a read whose caller would only swallow it.
     * @param bool $reportFailuresKeepingTheConnection Report a frame's failure that leaves the connection open,
     *        such as an -ERR the server keeps it open for, instead of throwing it: for a read whose caller would
     *        only swallow it ({@see readIncomingForOperation()}), and for the flushes of drain() and
     *        drainSubscription(), which read on to their PONG.
     * @param bool $deliverLeftovers Deliver what an earlier or a concurrent read has queued and not reached,
     *        before reading and after waiting for another fiber's read: for a serving loop's read
     *        ({@see readIncomingForOperation()}), since nothing else would deliver all of it until the server
     *        sent more - unless it is a close's to handle ({@see leftoversBelongToAClose()}). A delivery held up
     *        in another fiber's handler that awaits leaves the later sids queued until that handler returns, and
     *        a handler that threw leaves the rest of its own subscription queued ({@see deliverPending()}), which
     *        your own next read continues as well ($continueStoppedSubscriptions). Not for a flush: a handler it
     *        ran there could suspend while another fiber took the flush's PONG and started its next read, which
     *        the flush would then wait for until its deadline. A drain delivers that backlog itself.
     * @param bool $continueStoppedSubscriptions Continue, before reading and before the read-slot decision, the
     *        subscriptions whose delivery an earlier pass stopped at a handler failure it held to throw
     *        ({@see deliverStoppedSubscriptions()}, #186): for your own read ({@see readIncoming()}), which throws
     *        such failures one read at a time, so that what each one leaves queued does not wait for a read that
     *        receives anything. Only those: the later sids of a delivery held up in another fiber's handler that
     *        awaits stay queued for that delivery, as they do for every read but a serving loop's, unless a
     *        handler failure had already stopped one of them, which is continued whole, the messages that delivery
     *        brought for it included. With the read's own rules, so that a handler that throws there ends the read
     *        without reading; otherwise the read goes on as it would have, and its result counts only what it read.
     *        Not what a disconnect() already under way when the read starts is to discard
     *        ({@see leftoversBelongToAClose()}): a pass already running when one begins goes on. Not for an
     *        operation's read, which reports the other subscriptions' failures and takes its own subscription's
     *        queue ($ownSid): it leaves the remainders to your next read, or to whichever read receives anything
     *        first.
     * @param DeferredFuture<null>|null $pongSlot The pong slot this read is for: a flush's, or that of the PING behind
     *        the mux SUB, which a request waits for ({@see awaitMuxConfirmation()}). When it is complete by the time
     *        the read would take the read slot, or wait for another fiber's read, the read returns without reading.
     *        The caller checks for its PONG before each read, but the read runs on a fiber of its own, and before
     *        that fiber starts another fiber's read can take the PONG and free the read slot: the read then waited
     *        on a socket with nothing more to come until the caller's deadline. A slot still pending is also the
     *        read's wake-up ({@see wakeOnPong()}), unless $wake is given: a slot that completes while the read waits
     *        ends it as well, such as one that another fiber's dispatch completes once the write of a PONG it owed the
     *        server let it go on, or one that a reconnect fails, its PONG gone with the old socket.
     * @param Cancellation|null $wake An operation's wake-up: fires once what the operation waits for may have arrived
     *        since the operation last looked for it, whichever fiber's read delivered it. The caller takes it in its own
     *        fiber, right after it looked. The read then ends without reading, and returns as one that waited for
     *        another fiber's read does, wherever it is: about to wait for a reconnect, waiting for one, about to take
     *        the read slot, waiting for another fiber's read, or waiting on the socket, whose read it cancels. A
     *        transport's read that is cancelled has consumed nothing: what it had not returned is still there for the
     *        next read. Not once the read has bytes: it delivers them, as any read does. A read with a wake-up, an
     *        operation's or a flush's for its PONG, also starts a lame-duck failover that its chunk brings in a fiber
     *        of its own, rather than run it inline as a read without one does, and once it has delivered the chunk it
     *        waits for that failover only within the operation's wait, unless the PONG it is for came in the chunk,
     *        as it waits for a reconnect its failed read starts (#178, #191): the deadline ends that wait with the
     *        caller's timeout, and the wake-up with the read returning what it read ({@see handleServerInfoUpdate()},
     *        {@see awaitLameDuckFailoverFrom()}). Not for a chunk that fails to parse: the frames ahead of the bad
     *        bytes are dispatched as a read without a wake-up dispatches them, a failover among them inline, and the
     *        read then recovers the corrupt stream.
     * @return Future<IncomingChunkResult>
     *
     * @phpstan-impure Mutates connection state, like readIncoming().
     */
    private function readChunk(
        ?Cancellation $cancellation,
        ?\Fiber $caller,
        bool $reportOverflows,
        ?int $ownSid = null,
        bool $reportHandlerFailures = false,
        bool $reportFailuresKeepingTheConnection = false,
        bool $deliverLeftovers = false,
        ?DeferredFuture $pongSlot = null,
        ?Cancellation $wake = null,
        bool $continueStoppedSubscriptions = false,
    ): Future {
        if ($wake === null && $pongSlot !== null && !$pongSlot->isComplete()) {
            // A slot still pending is the read's wake-up. One already complete is not: that read returns at the slot
            // check below, once any reconnect it waits for is over. Made its wake-up, it fired before the read began, and
            // a caller that loops until something else happens (awaitMuxConfirmation()) polled through the whole
            // reconnect, a look every millisecond.
            $wake = $this->wakeOnPong($pongSlot);
        }

        return async(function () use ($cancellation, $caller, $reportOverflows, $ownSid, $reportHandlerFailures, $reportFailuresKeepingTheConnection, $deliverLeftovers, $pongSlot, $wake, $continueStoppedSubscriptions): IncomingChunkResult {
            // What the read waits with: the caller's cancellation, and the operation's wake-up. Every wait below
            // subscribes to it before it suspends, and the checks before each wait run with no suspension between
            // them and the wait, so a wake-up that fires at any point either ends the wait or is seen by a check.
            $waitCancellation = self::waitWith($cancellation, $wake);

            // Delivered during the hops before this fiber started.
            if (self::wokenUp($wake, $cancellation)) {
                return new IncomingChunkResult(0, false);
            }

            if ($this->state !== ConnectionState::Open && $this->state !== ConnectionState::Draining) {
                // The recovery future resolves only once the recovery has finalized the state, so a
                // reader waiting here still never touches the new socket during the subscription
                // replay window (#148).
                try {
                    $this->awaitOpenConnection($waitCancellation, $caller, acceptDraining: true);
                } catch (CancelledException $cancelled) {
                    // Delivered while the connection was down: by the reconnect's delivery of what it read, or by
                    // a delivery still under way when the connection dropped.
                    if (!self::wokenUp($wake, $cancellation)) {
                        // The caller's own exception when both fired: the wake-up's may have won inside the composite.
                        $cancellation?->throwIfRequested();

                        throw $cancelled;
                    }

                    return new IncomingChunkResult(0, false);
                }

                // An operation's read that waited for a reconnect returns without reading, whether or not the
                // reconnect delivered anything to it: the operation may have to act on the new connection first, as
                // the pull engine re-issues the pulls the old server forgot, and would otherwise read the new socket
                // until the server's next bytes or its deadline before it looked.
                if ($wake !== null) {
                    return new IncomingChunkResult(0, false);
                }
            }

            // A serving loop's read first delivers what an earlier or a concurrent read has queued and not reached:
            // the later sids of a delivery that another fiber's handler holds up, awaiting, or the rest of a
            // subscription whose handler threw in the application's own read (#177), which the application's next
            // read also continues (below). A read that finds nothing new delivers nothing, so that would otherwise
            // wait for the server to send more. Done before the check below, so that a handler that suspends cannot
            // leave two fibers reading at once. Such a handler can outlast the connection, too: a recovery started
            // meanwhile is waited for, as above, before the socket is read, or this read would take the replies to
            // the recovery's handshake. What a disconnect() under way is to discard is left to it, as the delivery
            // after a reconnect leaves it, and that is checked before every pass: a handler that suspends can let a
            // close begin.
            while ($deliverLeftovers && $this->pendingDirty !== [] && !$this->leftoversBelongToAClose()) {
                $this->deliverPending($reportOverflows, $ownSid, $reportHandlerFailures);
                if ($this->state === ConnectionState::Open || $this->state === ConnectionState::Draining) {
                    break;
                }

                $this->awaitOpenConnection($cancellation, $caller, acceptDraining: true);
            }

            // Your own read first continues the subscriptions whose delivery an earlier pass stopped at a handler
            // failure it held to throw (#186): the rest of each is queued, in order, and would otherwise wait for a
            // read that receives anything, on an otherwise idle connection the server's next PING. Only those: the
            // later sids of a delivery held up in another fiber's handler stay queued for that delivery, which goes on
            // once the handler returns, unless a handler failure had already stopped one of them, which is continued
            // whole. With this read's own rules, so that a handler that throws there ends the read without reading;
            // otherwise the read goes on below, as it would have, and its result counts what it read. With the guards
            // of the leftovers above: a handler that suspends cannot leave two fibers reading at once, a recovery
            // started meanwhile is waited for before the socket is read, and what a disconnect() already under way is
            // to discard is left to it. A pass already running when a disconnect() begins goes on to the other stopped
            // subscriptions, as the one above does.
            while ($continueStoppedSubscriptions && $this->stoppedByAFailure !== [] && !$this->leftoversBelongToAClose()) {
                $this->deliverStoppedSubscriptions($reportOverflows);
                if ($this->state === ConnectionState::Open || $this->state === ConnectionState::Draining) {
                    break;
                }

                $this->awaitOpenConnection($cancellation, $caller, acceptDraining: true);
            }

            // An operation's read first takes what an earlier read has queued for the operation's own subscription
            // and not reached (#179): a message behind a delivery held up in the handler of a lower sid, awaiting. That
            // handler can be the one the operation runs in, a handler polling a SubscriptionQueue, whose message came
            // in the same chunk as the handler's own: nothing would deliver it before the handler returned, and the
            // handler waits for it. Only that subscription is delivered, the others' queued messages stay in order for
            // the delivery under way, and the read returns without reading, so that the operation looks again. Not a
            // subscription whose own delivery is under way, which delivers it, and not what a disconnect() under way
            // is to discard, as for the leftovers above.
            if ($ownSid !== null && $this->deliverQueuedForOwnSid($ownSid, $reportOverflows, $reportHandlerFailures)) {
                return new IncomingChunkResult(0, false);
            }

            // The PONG this read is for is already in: another fiber's read took it after the caller last checked.
            if ($pongSlot?->isComplete() ?? false) {
                return new IncomingChunkResult(0, false);
            }

            // Delivered by the reconnect waited for above, or by another fiber's read meanwhile.
            if (self::wokenUp($wake, $cancellation)) {
                return new IncomingChunkResult(0, false);
            }

            if ($this->readInProgress) {
                // Another fiber owns the socket read - a request waiting for its reply, the heartbeat, a
                // flush - and the transport allows one read at a time. Wait for that read to finish,
                // bounded by this read's cancellation, and return without reading: it delivers what it
                // reads. Returning at once let a loop of reads run on futures that finish straight away,
                // which never hand the event loop control: no timer or socket read ran again, that other
                // read's included, and the process spun at 100% CPU. An operation's wake-up ends the wait
                // too: what that read delivers can be all the operation waits for, and the read can go on
                // for a long time after, in a handler that awaits.
                try {
                    $this->readSlotReleased->getFuture()->await($waitCancellation);
                } catch (CancelledException $cancelled) {
                    if (!self::wokenUp($wake, $cancellation)) {
                        $cancellation?->throwIfRequested();

                        throw $cancelled;
                    }

                    return new IncomingChunkResult(0, false);
                }

                // Unless its delivery is held up in a handler, or stopped at a throwing one: what it has not reached
                // is delivered here, as above.
                if ($deliverLeftovers && $this->pendingDirty !== [] && !$this->leftoversBelongToAClose()) {
                    $this->deliverPending($reportOverflows, $ownSid, $reportHandlerFailures);
                }

                // And what that read queued for the operation's own subscription behind a handler it is held up in,
                // as above (#179): the operation then finds it as it looks again, rather than reading once more first.
                if ($ownSid !== null) {
                    $this->deliverQueuedForOwnSid($ownSid, $reportOverflows, $reportHandlerFailures);
                }

                return new IncomingChunkResult(0, false);
            }

            $this->readInProgress = true;
            $generation = $this->connectionGeneration;

            $readError = null;
            try {
                $chunk = $this->transport->readLine($waitCancellation)->await();
            } catch (CancelledException $cancelledException) {
                if (!self::wokenUp($wake, $cancellation)) {
                    $cancellation?->throwIfRequested();

                    throw $cancelledException;
                }

                // Delivered by a delivery still under way in another fiber - one whose handler awaits, or whose
                // dispatch waits for the PONG it owes the server - while this read waited on the socket: it ends
                // with nothing read, and the bytes still on the socket stay there for the next read.
                $chunk = '';
            } catch (\Throwable $e) {
                $readError = $e;
                $chunk = '';
            } finally {
                // Released before a failed read recovers below. The recovery runs listeners and delivers
                // what arrived meanwhile, and a read issued from any of them, or parked behind this one,
                // must not wait for this read, which cannot end before the recovery does. No reader can
                // touch the new socket meanwhile: the recovery keeps the state Connecting until it is done
                // (#148), and a reader woken here returns without reading.
                $this->readInProgress = false;
                $this->signalReadSlotFree();
            }

            if ($readError !== null) {
                // During drain() a read failure means the flush is finished, not a fault to recover
                // from: recovering would reconnect and re-SUBscribe the very subscriptions drain()
                // just UNSUBbed (and could re-deliver). Treat it as end-of-flush instead.
                if ($this->state !== ConnectionState::Draining) {
                    // emitErrorSafely: a throwing user logger must not skip the recovery below - the
                    // connection would stay Open on a dead socket, failing every later read the same way.
                    $this->emitErrorSafely($readError);
                    // A recovery another fiber already runs (typically the heartbeat's) is joined only
                    // until this read's own cancellation fires: a request whose read failed must not
                    // outlive its timeout waiting for the whole backoff schedule. Nor an operation's
                    // read its wake-up: what it waits for can come meanwhile, in a delivery still under
                    // way when the connection dropped. A read with a wake-up - an operation's, or a
                    // flush's for its PONG - waits only that long for a recovery it starts itself as well, which
                    // then runs in a fiber of its own, and with waiting disabled fails at once (#178); a read
                    // without one - your own, a serving loop's - runs it here, and waits for all of it.
                    try {
                        if ($wake !== null) {
                            $this->recoverAfterFailedOperationRead($generation, $readError, $waitCancellation);
                        } else {
                            $this->recoverConnection(joinCancellation: $waitCancellation, failedGeneration: $generation, cause: $readError);
                        }
                    } catch (CancelledException $cancelled) {
                        if (!self::wokenUp($wake, $cancellation)) {
                            $cancellation?->throwIfRequested();

                            throw $cancelled;
                        }
                    }
                } else {
                    // ...and end it now: the PONG the flush waits for died with the socket, so the flush
                    // would otherwise keep reading a dead socket until drain()'s budget ran out.
                    $this->failPongWaiters(new ConnectionException('Connection lost before the server answered the PING'));
                }

                return new IncomingChunkResult(0, false);
            }

            // Done waiting: let go of the composite, so that what this read delivers to its own subscription fires a
            // wake-up nobody is subscribed to, with no CancelledException made and no callback queued.
            $waitCancellation = null;

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

                // Whatever a frame or a handler throws meanwhile, the recovered frames are delivered and the
                // corrupt stream is reported and recovered; then the first failure reaches the caller (#128
                // rethrow-after-containment) and a later one is reported rather than hiding it.
                $dispatchError = null;
                try {
                    $this->dispatchFrames($recovered, $reportOverflows, $ownSid);
                } catch (\Throwable $e) {
                    $dispatchError = $e;
                }

                $failure = $dispatchError;
                try {
                    // Deliver the enqueued messages even when a frame failed to dispatch, mirroring the
                    // clean-path drain below.
                    $this->deliverPending($reportOverflows, $ownSid, $reportHandlerFailures);
                } catch (\Throwable $e) {
                    if ($failure === null) {
                        $failure = $e;
                    } else {
                        $this->emitErrorSafely($e);
                    }
                }

                // A frame's failure that leaves the connection open is reported instead, when the read reports
                // such failures, as on the clean path below: an -ERR read ahead of the corrupt line must not
                // end a drain's flush, or reach a serving loop that would only swallow it.
                if ($dispatchError !== null && $reportFailuresKeepingTheConnection && !$this->frameFailureEndsConnection($dispatchError)) {
                    $this->emitErrorSafely($dispatchError);
                    $failure = null;
                }

                // A handler that throws while the recovered frames are delivered must not leave the
                // connection Open on a corrupt stream with the failure unobservable: the error emission and
                // the recovery run regardless.
                $this->emitErrorSafely($parseError);
                try {
                    // A recovery another fiber already runs is joined only until this read's own cancellation
                    // fires, as on the read-failure path above.
                    $this->recoverConnection(joinCancellation: self::waitWith($cancellation, $wake), failedGeneration: $generation, cause: $parseError);
                } catch (CancelledException $cancelled) {
                    // The read's deadline ended the wait, unless the read's wake-up did: then the recovered frames are
                    // delivered and the operation looks again. A failure already on its way still wins.
                    if (!self::wokenUp($wake, $cancellation) && $failure === null) {
                        $cancellation?->throwIfRequested();

                        throw $cancelled;
                    }
                } catch (\Throwable $recoveryError) {
                    if ($failure !== null) {
                        $this->emitErrorSafely($failure);
                    }

                    throw $recoveryError;
                }

                if ($failure !== null) {
                    throw $failure;
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
                // A read with a wake-up starts a lame-duck failover in a fiber of its own, waited for below (#191).
                $this->dispatchFrames($frames, $reportOverflows, $ownSid, failOverInItsOwnFiber: $wake !== null);
            } catch (\Throwable $e) {
                // Held, not rethrown yet: the already-enqueued backlog must still drain (wire-order
                // delivery, #128) before this frame error reaches the caller below, recovering the
                // connection first when it ends it (a fatal -ERR, a failed PONG write).
                $dispatchError = $e;
            }

            try {
                // Drain buffered deliveries after each chunk to preserve wire-order delivery - even
                // when a frame failed to dispatch: the messages are already enqueued and their bytes
                // consumed, so they must not wait behind (or be lost to) the surfacing error.
                $this->deliverPending($reportOverflows, $ownSid, $reportHandlerFailures);
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

            // A lame-duck failover away from the connection this read read on, started in a fiber of its own by this
            // read's dispatch or by another fiber's read of the same connection, is waited for once the chunk is
            // delivered, and only within the operation's own wait, as a reconnect its failed read starts is (#178,
            // #191): the deadline ends the wait with the caller's timeout and the wake-up with the read returning what
            // it read, while the failover carries on. A read whose dispatch failed throws that at once instead. Not a
            // read whose PONG came in the chunk: it has what it waits for, though its wake-up (wakeOnPong()) fires only
            // from a queued callback, too late for the wait to see it, and with waiting disabled a flush that had its
            // PONG failed with "Connection is not open".
            if ($dispatchError === null && $wake !== null && !($pongSlot?->isComplete() ?? false)) {
                $this->awaitLameDuckFailoverFrom($generation, $cancellation, $wake);
            }

            if ($dispatchError !== null) {
                // A frame that ends the connection recovers it before its error reaches the caller, as a
                // failed read does (#171). During drain() the close intent keeps that recovery from
                // reconnecting: drain() reports the error and closes, as it does when the socket fails.
                if ($this->frameFailureEndsConnection($dispatchError)) {
                    $this->recoverAfterEndingFrame($generation, $dispatchError, $cancellation);
                } elseif ($reportFailuresKeepingTheConnection) {
                    $this->emitErrorSafely($dispatchError);

                    return new IncomingChunkResult(count($frames), true);
                }

                throw $dispatchError;
            }

            // Bytes were consumed off the wire this read (the chunk was non-empty); $frames may still
            // be 0 when a large frame spans several chunks and this one did not complete it (#119).
            return new IncomingChunkResult(count($frames), true);
        });
    }

    /**
     * Whether what an earlier or a concurrent read has queued and not reached is a close's to handle, not a serving
     * loop's read's to deliver ({@see readChunk()}): close-intent is set and the connection is not Draining. A disconnect() under way
     * discards it - delivered, it reached handlers after disconnect() had been called - and a drain() without a
     * connection delivers it itself. A drain() that is Draining takes the backlog over and lets reads deliver as
     * they go, as its flush's reads do. Only the deliveries a read makes before it reads ask this - a serving loop's
     * leftovers, an operation's take of its own subscription ({@see deliverQueuedForOwnSid()}), and your own read's
     * continuation of the subscriptions a handler failure stopped ({@see deliverStoppedSubscriptions()}): a read
     * that receives anything during the close delivers the whole queue with what it received, as any read does
     * ({@see deliverPending()} does not look at close-intent). They ask it before each pass, not within one: a pass
     * under way when the close begins, in a handler that awaits, goes on to the end.
     */
    private function leftoversBelongToAClose(): bool
    {
        return $this->closing && $this->state !== ConnectionState::Draining;
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
     * replays SUBs, not missed messages) (#128). One failure is rethrown after every frame has been
     * dispatched, the one that matters most to the caller ({@see dispatchFailureRank()}): a failure that
     * ends the connection (a fatal -ERR, a failed PONG write) outranks any other met earlier in the chunk,
     * which outranks a full subscription queue; among equals the first wins. The caller decides from it what
     * happens to the connection, and the others are reported instead.
     *
     * The frames are the server's that the chunk was read from, and the chunk's connection can be gone before
     * the last of them is handled (#182): a lame-duck INFO among them fails the connection over inline
     * ({@see handleServerInfoUpdate()}), so handling it returns with the connection replaced
     * ({@see $connectionGeneration} moved on) or ended ({@see $terminalCloses} moved on: the failover reached no
     * server, or the LameDuck listener closed the connection itself, with a disconnect(), say, which leaves no
     * failover to run and moves the close count alone). For a chunk that a read with a wake-up read and parsed,
     * the INFO's dispatch starts the failover in a fiber of its own instead ($failOverInItsOwnFiber, #191) and
     * returns before that fiber has run, the connection neither replaced nor ended yet but being left:
     * {@see $lameDuckFailover} names it until the failover returns, and from then on the rest of the chunk, and of
     * any chunk read on that connection by any fiber, is the leaving server's as well. The rest of the chunk is
     * then the old server's, and is not applied to the new connection ({@see handleStaleFrame()}): its messages
     * are still queued for their subscriptions, which the reconnect replays, though a delivery on the reply inbox
     * no longer counts as the new server's confirmation of the replayed inbox ({@see handleFrame()}, its $stale);
     * its PING is answered with a PONG on neither connection; its PONG completes no slot of the new connection's,
     * though while the connection is only being left it still completes that connection's oldest slot, its own
     * PING's; its INFO neither overwrites the new server's info nor starts a second failover; and its -ERR is
     * reported instead of being applied to the new connection or failing the read. Checked before each frame,
     * since the connection goes while one of them is handled; here, so that every caller gets the rule.
     *
     * @param list<ProtocolFrame> $frames
     * @param bool $reportOverflows Report a full subscription queue ({@see SlowConsumerException})
     *        through the error listener instead of rethrowing it - for a caller that must not fail on
     *        it: a reconnect has nobody to throw it to, a drain needs the messages behind it, an operation
     *        waits for a result of its own - so only the failures that matter to that caller, such as a
     *        fatal -ERR, are rethrown.
     * @param int|null $ownSid The caller's own subscription, whose overflow is rethrown all the same.
     * @param bool $failOverInItsOwnFiber Start a lame-duck failover that an INFO of the chunk brings in a fiber of
     *        its own rather than run it inline ({@see handleServerInfoUpdate()}): for a read with a wake-up, an
     *        operation's or a flush's, which waits for it only within the operation's own wait ({@see readChunk()},
     *        #191). Your own read, a serving loop's, the heartbeat's and those of a connect or a reconnect run it
     *        inline, and so does any read for the frames it parsed ahead of bytes that fail to parse, whose corrupt
     *        stream it recovers from inline right after.
     */
    private function dispatchFrames(array $frames, bool $reportOverflows = false, ?int $ownSid = null, bool $failOverInItsOwnFiber = false): void
    {
        $firstError = null;
        /** @var list<array{\Throwable, string}> $reports What to report, with its log level. */
        $reports = [];
        // The connection the chunk was read on (#182).
        $generation = $this->connectionGeneration;
        $closes = $this->terminalCloses;

        foreach ($frames as $frame) {
            // The chunk's connection is gone once a frame handled before this one has replaced or ended it.
            $replaced = $this->connectionGeneration !== $generation || $this->terminalCloses !== $closes;
            // Or it is being left, by a lame-duck failover started in a fiber of its own that has not replaced it yet
            // (#191): an earlier frame of this chunk started it, or another fiber's read of the same connection did.
            $stale = $replaced || ($this->lameDuckFailover !== null && $this->lameDuckFailover['generation'] === $generation);
            if ($stale && !$this->handleStaleFrame($frame, $reports, $replaced)) {
                continue;
            }

            try {
                $this->handleFrame($frame, $reports, $stale, $failOverInItsOwnFiber);
            } catch (\Throwable $e) {
                $reportable = $reportOverflows && $e instanceof SlowConsumerException && $e->sid !== $ownSid;
                if (!$reportable && ($firstError === null || $this->dispatchFailureRank($e) > $this->dispatchFailureRank($firstError))) {
                    // The first failure, or one that matters more than the one held: a fatal -ERR after a
                    // non-closing one, which the server sends ahead of closing, say, when a rejected SUB's
                    // answer sat unread; any failure after an overflow, which only says that a subscriber fell
                    // behind. The one held is reported instead.
                    if ($firstError !== null) {
                        $reports[] = [$firstError, 'error'];
                    }

                    $firstError = $e;

                    continue;
                }

                // Only one failure is rethrown (below): the one that matters most, the first among equals.
                // Any other failure from the same chunk would otherwise vanish - neither thrown nor observable -
                // hiding the corresponding message loss from the error listener and from tests. Surface it
                // (contained so a throwing logger cannot mask the error being rethrown) (#158), like an
                // overflow this caller reports instead of throwing.
                $reports[] = [$e, 'error'];
            }
        }

        // Reported once the whole chunk is queued - with what the frames reported themselves, such as a
        // message a drop policy discarded or a recoverable -ERR: the listener runs user code, and one that
        // reads on the connection must find this chunk's messages queued ahead of the next chunk's, in wire
        // order. Guarded against a throwing logger, which would otherwise fail the read, or keep a message
        // a drop policy had already made room for from its queue.
        foreach ($reports as [$report, $logLevel]) {
            $this->emitErrorSafely($report, $logLevel);
        }

        if ($firstError !== null) {
            throw $firstError;
        }
    }

    /**
     * How much a failure met while dispatching a chunk matters to the caller ({@see dispatchFrames()}): one
     * that ends the connection most, a full subscription queue least.
     */
    private function dispatchFailureRank(\Throwable $failure): int
    {
        if ($this->frameFailureEndsConnection($failure)) {
            return 2;
        }

        return $failure instanceof SlowConsumerException ? 0 : 1;
    }

    /**
     * Decides what becomes of a frame read on a connection that is gone by the time the frame is handled, the
     * frames behind a lame-duck INFO in its chunk ({@see dispatchFrames()}, #182): the old server's, read before
     * the failover, with the connection now on another server, or ended because no server could be reached, or
     * closed by the LameDuck listener itself (a disconnect() called from it), which leaves no failover to run.
     * Or read on a connection that is being left: a failover started in a fiber of its own for a read with a
     * wake-up has yet to replace it (#191), and the frames are the leaving server's just the same, the new
     * connection about to be dialled. Returns whether {@see handleFrame()} is still to handle the frame.
     *
     * - MSG and HMSG are, and so is +OK, which handleFrame() ignores. The messages are the old server's deliveries,
     *   which the reconnect does not replay, for subscriptions it replays, so they are queued as any message is,
     *   and handed to handleFrame() as stale: a late reply on the reply inbox still reaches the request waiting for
     *   it, but does not confirm the replayed inbox, since a delivery of the server the connection left says nothing
     *   about whether the new server holds the replayed SUB, no more than that server's PONG does. One for an
     *   unknown sid is discarded, as always; after a failed failover, or a close, every sid is unknown, the close
     *   having released them, so the old server's last messages go with the connection, as the backlog a close
     *   discards does.
     * - PING is dropped: a PONG written for it would go to the new server, which asked for none, and the old
     *   server has no connection left to answer on, or, while the connection is only being left, is about to lose
     *   it to the failover.
     * - PONG is dropped once the connection is replaced or ended: it answers a PING of the old connection, whose
     *   slot is gone, since the recovery fails every slot of the old connection at its first attempt
     *   ({@see connectOnce()} calls {@see failPongWaiters()} before it dials) and a terminal close does the same
     *   ({@see releaseRuntimeState()}). The slots queued since are the new connection's, the fence behind the
     *   replayed reply-inbox SUB among them, and the stale PONG would complete the oldest of those: the reply inbox
     *   would count as confirmed before the new server had answered anything. Nor does the old server's PONG say
     *   anything about the new server, so the heartbeat's count is left alone. While the connection is only being
     *   left, the PONG goes on to handleFrame() and completes that connection's oldest slot, the one of the PING it
     *   answers: the slots are still that connection's until the failover's first attempt fails them, in the call
     *   that also moves the generation. Dropped, it would leave its slot to the next PONG, every later answer one
     *   slot off for good when the failover turns out not to run (close-intent set before its fiber ran) and the
     *   connection stays in use, and a flush() whose PONG came behind the INFO would fail for nothing.
     * - INFO is dropped: it describes the old server, and would overwrite {@see $serverInfo} with that, or, while
     *   the connection is only being left, change it and the pool under the failover about to dial; a second
     *   lame-duck INFO would start a second failover, since the new connection has its own lame-duck flag.
     * - -ERR is reported through the error listener, as a NatsException saying the server the connection left sent
     *   it, with the server's text, and nothing else: not applied to the new connection - a 'maximum subscriptions
     *   exceeded' would drop the replayed, still unconfirmed reply inbox, a permissions violation would fire the
     *   replayed subscription's rejection handler - not marked as ending the connection, and not thrown, so the
     *   read that brought the chunk does not fail for an -ERR of a server the connection has already left, or is
     *   leaving.
     *
     * @param list<array{\Throwable, string}> $reports Collects the report of an -ERR, with its log level, for
     *        dispatchFrames() to report once the whole chunk is queued.
     * @param bool $replaced Whether the connection the frame was read on is replaced or ended, rather than only being
     *        left by a failover that has yet to replace it.
     * @param-out list<array{\Throwable, string}> $reports
     */
    private function handleStaleFrame(ProtocolFrame $frame, array &$reports, bool $replaced): bool
    {
        switch ($frame->type) {
            case ProtocolFrameType::Pong:
                // While the connection is only being left, its slots are still its own: the PONG completes the oldest.
                return !$replaced;
            case ProtocolFrameType::Ping:
            case ProtocolFrameType::Info:
                return false;
            case ProtocolFrameType::Err:
                $reports[] = [new NatsException('The server the connection left sent an error frame: ' . ($frame->error ?? 'unknown')), 'error'];

                return false;
            default:
                return true;
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
     * One timeout bounds the whole request. While a reconnect is in flight the request first waits for
     * it within that timeout ({@see NatsOptions::$waitForReconnect}).
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
        $caller = \Fiber::getCurrent();

        return async(function () use ($subject, $payload, $timeoutMs, $cancellation, $caller): NatsMessage {
            // Cached: request targets repeat (unlike the per-request inbox, which is validated
            // uncached as publish()'s replyTo).
            $this->validateSubjectCached($subject);

            return $this->requestInternal($subject, $payload, null, $timeoutMs, $cancellation, $caller);
        });
    }

    /**
     * Sends a request with headers and awaits the first response. Same timeout and reconnect-wait
     * semantics as {@see request()}.
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
        $caller = \Fiber::getCurrent();

        return async(function () use ($subject, $payload, $headers, $timeoutMs, $cancellation, $caller): NatsMessage {
            // Cached: request targets repeat (unlike the per-request inbox, which is validated
            // uncached as publish()'s replyTo).
            $this->validateSubjectCached($subject);

            return $this->requestInternal($subject, $payload, $headers, $timeoutMs, $cancellation, $caller);
        });
    }

    /**
     * Establishes the per-connection mux request inbox on first use: one wildcard subscription
     * "<base>.*" whose handler ({@see dispatchMuxReply()}) routes every reply to its per-token waiter.
     * Idempotent - a no-op once established, so it survives reconnect (muxSid/muxBase are retained until
     * a terminal close, or until the server rejects the SUB). Serialized via $muxInboxSetup so concurrent
     * first requests write exactly one SUB even though the subscribe suspends (#118).
     *
     * The mux is recorded before its SUB is written, so whichever fiber reads the server's answer to the
     * SUB knows it is the mux's (#167, {@see dropUnconfirmedMux()}). The same write unsubscribes the muxes
     * the client dropped before, and carries a PING right behind the SUB, whose PONG confirms the mux.
     * Nothing waits for that PONG here.
     *
     * Precondition: the caller has checked state === Open (the subscribe re-checks it). A request that
     * joins another fiber's establishment waits for it only within $budget - its own deadline.
     *
     * @param ?\Fiber<mixed, mixed, mixed, mixed> $caller The fiber that issued the request.
     */
    private function ensureMuxInbox(Cancellation $budget, ?\Fiber $caller): void
    {
        if ($this->muxInboxSetup !== null) {
            // Another fiber is mid-establishment; join it instead of writing a second SUB. Checked before
            // muxSid, which is set from just before that SUB is written, while the write may still be under way.
            $this->muxInboxSetup->await($budget);

            return;
        }

        if ($this->muxSid !== null) {
            return;
        }

        $base = Inbox::generate($this->options->inboxPrefix);
        $generation = $this->muxGeneration;
        $deferred = new DeferredFuture();
        $setup = $deferred->getFuture();
        $setup->ignore();
        $this->muxInboxSetup = $setup;

        try {
            $release = '';
            foreach ($this->muxSidsToRelease as $droppedSid) {
                $release .= $this->codec->encodeUnsubscribe($droppedSid);
            }
            $this->muxSidsToRelease = [];

            // dispatchMuxReply is non-suspending, so it is safe to invoke inside drainPendingForSid()'s
            // dequeue loop. The handler lives in subscriptions[$muxSid] and persists across reconnect
            // (resubscribeAll replays only the SUB bytes; only releaseRuntimeState clears the handler).
            $this->subscribeInternal(
                $base . '.*',
                $this->dispatchMuxReply(...),
                null,
                $caller,
                registered: function (int $sid) use ($base): void {
                    $this->muxBase = $base;
                    $this->muxSid = $sid;
                    $this->muxConfirmed = false;
                    // Slow-consumer exemption: the mux queue must never drop a reply (breaks a request).
                    $this->unboundedSids[$sid] = true;
                    // Queued in the step that hands its PING to the transport, as every PING's slot is, so that
                    // whatever PINGs other fibers write, this slot completes on the PONG answering that PING.
                    $this->muxFence = $this->enqueuePongSlot();
                },
                before: $release,
                after: $this->codec->encodePing(),
            );
            $deferred->complete();
        } catch (\Throwable $e) {
            if ($this->muxBase === $base) {
                // The subscribe rolled its registration back: the connection did not come back in time.
                $this->forgetMux();
            }

            // The reconnect a failed write started can also remove the registration, when its replay of the SUB
            // is rejected; the subscribe then reports a closed connection. Say what happened instead.
            if ($this->muxGeneration !== $generation) {
                $e = $this->muxDroppedException($e);
            } elseif ($this->muxRejected) {
                $e = $this->muxRejectedException($e);
            }

            $deferred->error($e);

            throw $e;
        } finally {
            // Cleared so a failed establishment retries on the next request - unless a terminal close or a drop
            // of the mux already let another request start its own.
            if ($this->muxInboxSetup === $setup) {
                $this->muxInboxSetup = null;
            }
        }
    }

    /**
     * Forgets the mux subscription, so the next request that needs one subscribes a new one. Its registration
     * is the caller's to drop.
     */
    private function forgetMux(): void
    {
        $this->muxSid = null;
        $this->muxBase = null;
        $this->muxConfirmed = false;
        $this->muxFence = null;
    }

    /** Records that the server holds the mux subscription ({@see $muxConfirmed}). */
    private function confirmMux(): void
    {
        $this->muxConfirmed = true;
        $this->awaitMuxFence = false;
    }

    /**
     * Drops the mux subscription when a 'maximum subscriptions exceeded' -ERR arrives before the server is known
     * to hold it ({@see $muxConfirmed}). That -ERR names no subject, but the server answers a SUB it rejects
     * ahead of the PONG of the PING written behind it, and a rejected SUB delivers nothing, so until then the -ERR
     * may be the mux's. Like the #167 latch, except that nothing is latched, since the limit is transient: the
     * requests waiting on the mux fail fast, and the next request subscribes a new one, which it waits to see
     * confirmed before it is sent.
     *
     * The mux is recorded before its SUB is written, so an -ERR for an earlier SUB that arrives while that write
     * still waits behind earlier writes drops it as well: the window covers that wait plus one round trip.
     *
     * The requests waiting on the dropped mux are woken last ({@see wakeMuxWaiters()}, #180), once the generation
     * they check has moved on, so that each of them fails at its next look, whichever fiber's read met the -ERR.
     */
    private function dropUnconfirmedMux(int $sid): void
    {
        $this->dropSubscriptionState($sid);
        $this->muxSidsToRelease[] = $sid;
        $this->forgetMux();
        $this->muxGeneration++;
        $this->awaitMuxFence = true;
        // A set-up whose write is still under way no longer sets a mux up: a request that joined it now would wait
        // for a confirmation nothing is coming for. The next request subscribes a new mux instead, and its write
        // follows the dropped one's on the socket, so the UNSUB in it comes after the dropped SUB.
        $this->muxInboxSetup = null;
        $this->wakeMuxWaiters();
    }

    /**
     * Records that the server holds the guarded inbox whose fence PONG completed $slot, if $slot is one
     * ({@see $unconfirmedSids}): the server answered the PING written behind that inbox's SUB without rejecting the
     * SUB first. The slot completes as any slot does, by the caller.
     *
     * @param DeferredFuture<null> $slot The pong slot the PONG just read completes.
     */
    private function confirmSubscriptionFencedBy(DeferredFuture $slot): void
    {
        $sid = array_search($slot, $this->unconfirmedSids, true);
        if ($sid !== false) {
            unset($this->unconfirmedSids[$sid]);
        }
    }

    /**
     * Treats every guarded inbox the server is not yet known to hold as rejected by the 'maximum subscriptions
     * exceeded' -ERR just read ({@see subscribeGuarded()}, #175), the way {@see dropUnconfirmedMux()} treats the reply
     * inbox: its handler is called with the -ERR text first, so that the operation finds the rejection recorded when
     * it looks again, then the subscription is dropped, which ends the operation's read waiting for a delivery on it
     * ({@see dropSubscriptionState()}, the wake-up of #174) wherever it waits, and the UNSUB the rejection owes is
     * queued ({@see $guardedSidsToRelease}), for the operation's unsubscribe() or, in a reconnect's replay, for the
     * replay to write. When several inboxes are unconfirmed at once none can be told apart, so each is treated as
     * rejected, as documented.
     *
     * Called in the dispatch of the -ERR, before the read that met it fails or reports, whichever fiber's read it
     * is: the operation's own, which then fails with the server's error as it always did, or another's.
     */
    private function rejectUnconfirmedSubscriptions(string $error): void
    {
        foreach (array_keys($this->unconfirmedSids) as $sid) {
            $handler = $this->subscriptionRejectionHandlers[$sid] ?? null;
            if ($handler !== null) {
                try {
                    $handler($error);
                } catch (\Throwable) {
                    // A throwing handler must never break frame dispatch.
                }
            }

            $this->dropSubscriptionState($sid);
            $this->guardedSidsToRelease[] = $sid;
        }
    }

    /**
     * Writes the UNSUB owed by a guarded inbox the client treated as rejected ({@see rejectUnconfirmedSubscriptions()}),
     * when the operation that owned it unsubscribes it ({@see unsubscribe()}): the -ERR may have been another SUB's
     * after all, and the server then still holds the inbox, in a slot this frees. On a connection that is not open
     * the sid is forgotten instead: the connection the server held it on is gone, or closing, and a replayed inbox
     * the new server rejected got its UNSUB from the replay ({@see finishReplayWindow()}). A failed write is not
     * thrown, as in unsubscribe(): the server dropped the subscription with the connection.
     */
    private function releaseRejectedSubscription(int $sid): void
    {
        if (!$this->takeOwedRelease($sid) || $this->state !== ConnectionState::Open) {
            return;
        }

        try {
            $this->transport->write($this->codec->encodeUnsubscribe($sid))->await();
        } catch (\Throwable) {
            // The socket is dead, and the server dropped the subscription with the connection.
        }
    }

    /**
     * Removes $sid from the UNSUBs owed by rejected guarded inboxes ({@see $guardedSidsToRelease}), for the caller
     * that writes its UNSUB; whether it was owed.
     */
    private function takeOwedRelease(int $sid): bool
    {
        if (!in_array($sid, $this->guardedSidsToRelease, true)) {
            return false;
        }

        $this->guardedSidsToRelease = array_values(array_filter(
            $this->guardedSidsToRelease,
            static fn(int $queued): bool => $queued !== $sid,
        ));

        return true;
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
     * Registers a request's reply-dispatch callback under its suffix token, before its PUB is written, together with
     * the wake-up of the request's read ({@see $muxWakes}): the request fires it when its reply is in, and the
     * connection fires it when it drops or rejects the reply inbox ({@see wakeMuxWaiters()}, #180). A waiter that
     * rotates its wake-up at each reply registers the new one with {@see renewMuxWake()}.
     *
     * @param callable(NatsMessage):void $handler
     * @param DeferredCancellation $wake What the request reads with next; fires once, like any wake-up.
     */
    private function registerMuxWaiter(string $token, callable $handler, DeferredCancellation $wake): void
    {
        $this->muxWaiters[$token] = $handler;
        $this->muxWakes[$token] = $wake;
    }

    /**
     * Replaces the wake-up registered for $token ({@see registerMuxWaiter()}): requestMany()'s waiter fires the one
     * its read waits with at each reply and reads on with a new one, which is then the one a drop of the inbox has
     * to fire. Called from the waiter, so the token is still registered.
     */
    private function renewMuxWake(string $token, DeferredCancellation $wake): void
    {
        $this->muxWakes[$token] = $wake;
    }

    /**
     * Removes a request's waiter, with its wake-up. Runs in the finally of both request methods, on every exit
     * path - the removal that makes a later/duplicate reply for a terminated request undeliverable (#118), and
     * that leaves no wake-up behind for a request that is gone (#180).
     */
    private function removeMuxWaiter(string $token): void
    {
        unset($this->muxWaiters[$token], $this->muxWakes[$token]);
    }

    /**
     * Ends the waits of the requests waiting on the reply inbox, once the client has dropped it
     * ({@see dropUnconfirmedMux()}) or latched its rejection by permissions (#167): each request's wait ends, wherever
     * it waits - in a read, on the socket, behind another fiber's read or for a reconnect, or at its own wait for the
     * read slot while another fiber's read holds it - and the request looks again, as it does when its wake-up fires
     * with its reply (#180). The top of its loop then decides as it always did: a reply delivered by then is returned,
     * otherwise the request fails with {@see muxRejectedException()} or {@see muxDroppedException()}, and a
     * requestMany() with replies collected returns them.
     *
     * Each request's own wake-up fires with its own reply alone, so a request whose read was parked on the socket while
     * ANOTHER fiber's read met the -ERR used to notice the drop only with the server's next bytes or at its deadline:
     * the shape arises when that fiber's dispatch is held up ahead of the -ERR, as it is while it awaits the write of
     * the PONG for a server PING in the same chunk, with the read slot already free. Called once the state
     * the loops check has changed, so that a woken request never looks and waits on: a wake-up fires once and stays
     * fired, so every wait the request makes after it ends at once, and the look after each must exit. The one state
     * change a close undoes, the rejection latch, is covered by the loops' exit on the close count
     * ({@see $terminalCloses}). One the reply fired already stays as it is, and with no request waiting there is
     * nothing to fire.
     *
     * Not called at a terminal close ({@see releaseRuntimeState()}), where the waiters are dropped instead: nothing
     * waits on past the close, and each loop exits on the close count.
     */
    private function wakeMuxWaiters(): void
    {
        foreach ($this->muxWakes as $wake) {
            $wake->cancel();
        }
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
    private function muxRejectedException(?\Throwable $previous = null): ConnectionException
    {
        $wildcard = $this->options->inboxPrefix . '.>';

        return new ConnectionException(
            'request/reply is unavailable on this connection: the shared reply-inbox subscription "'
            . $this->options->inboxPrefix . '.<inbox>.*" was rejected by the server (permissions violation). '
            . 'Grant the account subscribe permission for the reply-inbox wildcard "' . $wildcard
            . '" to use request()/requestMany().',
            0,
            $previous,
        );
    }

    /**
     * The error of a request whose mux reply-inbox subscription the client dropped because the server may have
     * rejected it for the subscription limit ({@see dropUnconfirmedMux()}): no reply can come on it. Unlike the
     * permissions rejection of {@see muxRejectedException()} this is not latched, since a slot can free up.
     */
    private function muxDroppedException(?\Throwable $previous = null): ConnectionException
    {
        return new ConnectionException(
            'request/reply failed: the server may have rejected the shared reply-inbox subscription "'
            . $this->options->inboxPrefix . '.<inbox>.*" because the connection is at its subscription limit '
            . '(maximum subscriptions exceeded). The next request subscribes it again.',
            0,
            $previous,
        );
    }

    /**
     * Throws when the mux subscription a request is about to be sent on is gone: released by a terminal close
     * while it was being set up, latched as rejected by permissions (#167), or dropped because the server may
     * have rejected it ({@see dropUnconfirmedMux()}).
     *
     * @param int $closes The {@see $terminalCloses} when the request began setting the mux up.
     * @param int $generation The {@see $muxGeneration} then.
     *
     * @phpstan-impure Reads state that reads in other fibers change while the request is suspended.
     */
    private function throwUnlessMuxInstalled(int $closes, int $generation): void
    {
        if ($this->terminalCloses !== $closes) {
            throw new ConnectionException('Connection was closed while the reply inbox was being set up');
        }

        if ($this->muxRejected) {
            throw $this->muxRejectedException();
        }

        if ($this->muxGeneration !== $generation) {
            throw $this->muxDroppedException();
        }
    }

    /**
     * Waits until the server is known to hold the mux subscription ({@see $muxConfirmed}), as a request does
     * before it is sent on a mux subscribed after the client dropped one ({@see $awaitMuxFence}). It reads the
     * way the request reads for its reply, except that its read returns without reading once the PONG that confirms
     * the mux is in, whichever fiber's read took it. When the server rejects the SUB again, the mux is dropped and the
     * request fails without being sent; when this read brought that -ERR, it is the previous exception instead
     * of being thrown, so it fails the request once and is not reported on top.
     *
     * @throws CancelledException When $budget fires first.
     */
    private function awaitMuxConfirmation(Cancellation $budget, int $closes, int $generation): void
    {
        while (!$this->muxConfirmed) {
            try {
                $read = $this->readChunk(
                    $budget,
                    \Fiber::getCurrent(),
                    reportOverflows: !$this->options->slowConsumerErrorsFailOperations,
                    reportHandlerFailures: !$this->options->handlerErrorsFailOperations,
                    pongSlot: $this->muxFence,
                )->await();
            } catch (ConnectionException $readError) {
                // The -ERR that dropped the mux becomes the previous exception of the dropped-inbox error, unless the
                // connection ended meanwhile: then the read's own error says best why.
                if ($this->terminalCloses === $closes && $this->muxGeneration !== $generation) {
                    throw $this->muxDroppedException($readError);
                }

                throw $readError;
            }

            $this->throwUnlessMuxInstalled($closes, $generation);

            if (!$read->consumedBytes) {
                delay(0.001, cancellation: $budget);
            }
        }
    }

    /**
     * The error a request reports when its budget ran out before the request could be sent: the
     * caller's own cancellation surfaces as CancelledException (as in the reply wait), anything else as
     * a timeout naming the subject and what the request was still $waitingFor.
     */
    private function requestNotSentFailure(string $subject, ?Cancellation $cancellation, string $waitingFor): \Throwable
    {
        if ($cancellation !== null && $cancellation->isRequested()) {
            return new CancelledException();
        }

        return new TimeoutException('Request timed out for subject ' . $subject . ' while waiting for ' . $waitingFor);
    }

    /**
     * The part of {@see requestInternal()}/{@see requestManyInternal()} before the publish: wait for an
     * in-flight reconnect, then make sure the mux reply inbox exists - both within the request's budget.
     * After the client dropped a mux the server may have rejected, it also waits for the new one to be
     * confirmed, so that a request whose inbox the server rejects again is not sent. Nor is a request whose
     * set-up a drain() overtook, since the drain has unsubscribed the inbox.
     *
     * @param ?\Fiber<mixed, mixed, mixed, mixed> $caller
     * @return int The {@see $muxGeneration} the request is sent on: when it changes, the mux was dropped.
     */
    private function prepareRequest(string $subject, Cancellation $budget, ?Cancellation $cancellation, ?\Fiber $caller): int
    {
        if ($this->state !== ConnectionState::Open) {
            try {
                $this->awaitOpenConnection($budget, $caller);
            } catch (CancelledException) {
                throw $this->requestNotSentFailure($subject, $cancellation, 'the connection to be re-established');
            }
        }

        if ($this->muxRejected) {
            throw $this->muxRejectedException();
        }

        $closes = $this->terminalCloses;
        $generation = $this->muxGeneration;
        try {
            $this->ensureMuxInbox($budget, $caller);
            // The server can answer the SUB before this request resumes from writing it, or from joining the
            // fiber that writes it.
            $this->throwUnlessMuxInstalled($closes, $generation);

            if ($this->awaitMuxFence) {
                $this->awaitMuxConfirmation($budget, $closes, $generation);
            }
        } catch (CancelledException) {
            throw $this->requestNotSentFailure($subject, $cancellation, 'the reply inbox to be set up');
        }

        // A drain() that began while the request set the inbox up, or waited for the server to take it, has unsubscribed
        // it: no reply can come. The request fails as one issued during the drain does, without being sent.
        if ($this->state === ConnectionState::Draining) {
            throw new ConnectionException('Connection is not open');
        }

        return $generation;
    }

    /**
     * Sends a request within its existing budget, with at most one recover-and-retry (#215).
     * Requests never enter the ordinary publish reconnect buffer: they wait for Open, then recheck
     * their deadline and reply inbox immediately before each transport write.
     *
     * A transport may suspend inline before write() returns its Future. Each attempt therefore runs
     * in its own fiber, and the request bounds its wait for that fiber. An already-started write is
     * uncancellable and may finish after expiry; no new write starts after expiry. A late failure can
     * still repair the connection independently, but cannot retry the abandoned request.
     *
     * @param array<string,string>|null $headers
     * @param ?\Fiber<mixed, mixed, mixed, mixed> $caller
     */
    private function publishRequestWithin(
        string $subject,
        string $payload,
        ?array $headers,
        string $replyTo,
        Cancellation $budget,
        ?Cancellation $cancellation,
        ?\Fiber $caller,
        int $closes,
        int $muxGeneration,
    ): void {
        $frame = $this->encodePublishFrame($subject, $payload, $headers, $replyTo);

        try {
            for ($attempt = 0; ; $attempt++) {
                $budget->throwIfRequested();
                $write = async(function () use ($frame, $payload, $budget, $caller, $closes, $muxGeneration): ?array {
                    $budget->throwIfRequested();
                    $this->awaitOpenConnection($budget, $caller);
                    // The scheduled writer may start after its waiter expired, or after a recovery
                    // dropped/rejected the mux. Never send on that expired or unusable request.
                    $budget->throwIfRequested();
                    $this->throwUnlessMuxInstalled($closes, $muxGeneration);
                    $generation = $this->connectionGeneration;

                    try {
                        $this->transport->write($frame)->await();
                    } catch (\Throwable $writeError) {
                        if ($budget->isRequested()) {
                            // The request may already have left. Repair only the failed generation;
                            // this continuation owns no retry and cannot damage a newer connection.
                            async(function () use ($generation, $writeError): void {
                                $this->recoverConnection(failedGeneration: $generation, cause: $writeError);
                            })->ignore();
                        }

                        return [$generation, $writeError];
                    }

                    $this->recordOutbound($payload);

                    return null;
                });
                $write->ignore();
                $failure = $write->await($budget);
                if ($failure === null) {
                    return;
                }

                [$generation, $writeError] = $failure;
                if ($attempt === 1) {
                    throw $writeError;
                }

                $this->recoverAfterFailedWrite($generation, $writeError, $budget, $caller);
            }
        } catch (CancelledException) {
            throw $this->requestNotSentFailure($subject, $cancellation, 'the request to be sent');
        }
    }

    /**
     * Executes request/reply flow using plain publish or header publish variants.
     *
     * One budget covers the whole request: the wait for an in-flight reconnect
     * ({@see NatsOptions::$waitForReconnect}), the mux inbox set-up, the publish and the wait for the
     * reply all draw on the deadline started here. The publish wait includes inline transport
     * backpressure and recovery from its own failed write; no retry starts after the budget expires.
     *
     * @param array<string,string>|null $headers
     * @param ?\Fiber<mixed, mixed, mixed, mixed> $caller The fiber that called request()/requestWithHeaders().
     */
    private function requestInternal(
        string $subject,
        string $payload,
        ?array $headers,
        ?int $timeoutMs,
        ?Cancellation $cancellation,
        ?\Fiber $caller = null,
    ): NatsMessage {
        $deadlineMs = $timeoutMs ?? $this->options->requestTimeoutMs;
        if ($deadlineMs <= 0) {
            throw new TimeoutException('Request timeout must be greater than zero');
        }

        $timeoutCancellation = new TimeoutCancellation($deadlineMs / 1000);
        $waitCancellation = $cancellation === null
            ? $timeoutCancellation
            : new CompositeCancellation($cancellation, $timeoutCancellation);

        $muxGeneration = $this->prepareRequest($subject, $waitCancellation, $cancellation, $caller);
        // The connection the request is sent on: a terminal close while the reply is awaited ends the request at its
        // next look ({@see $terminalCloses}).
        $closes = $this->terminalCloses;
        $token = $this->newMuxToken();
        $replyTo = $this->muxBase . '.' . $token;

        /** @var DeferredFuture<NatsMessage> $deferred */
        $deferred = new DeferredFuture();
        // Set by the handler when the reply is delivered. The wait loop checks this rather than
        // $deferred->isComplete() so a reply delivered in the same tick the deadline fires is
        // returned instead of being discarded as a spurious timeout.
        $replyReceived = false;
        // Fired with the reply: this request's read ends without reading, wherever it waits then, since the reply is
        // all it waits for. The reply can come in another fiber's delivery still under way, or in a reconnect's
        // (#174). Not a delivery to the reply inbox, which every request shares: that would end every request's read
        // at each reply. The connection fires it as well when it drops or rejects the inbox, on which no reply can
        // come then (#180): the checks below find that out.
        $replyArrived = new DeferredCancellation();
        // What the request waits with while it is parked behind another fiber's read, below: its budget and its
        // wake-up, so that a drop of the inbox ends that wait as it ends a read (#180).
        $parkedWith = new CompositeCancellation($waitCancellation, $replyArrived->getCancellation());

        // Registered by token on the shared mux inbox instead of a fresh per-request SUB (#118). The
        // handler body is unchanged and idempotent: a coalesced duplicate arriving in the same drain
        // batch (before the finally removes the waiter) is ignored by the isComplete() guard.
        $this->registerMuxWaiter($token, static function (NatsMessage $message) use ($deferred, &$replyReceived, $replyArrived): void {
            if (!$deferred->isComplete()) {
                $deferred->complete($message);
                $replyReceived = true;
                $replyArrived->cancel();
            }
        }, $replyArrived);

        try {
            $this->publishRequestWithin($subject, $payload, $headers, $replyTo, $waitCancellation, $cancellation, $caller, $closes, $muxGeneration);

            while (true) {
                // Completion is checked BEFORE the deadline so a reply delivered in the same tick the
                // deadline fires (by this loop's read or a concurrent heartbeat read) is returned
                // rather than discarded as a spurious timeout.
                if ($replyReceived) {
                    break;
                }

                // The connection closed for good while the request waited - a disconnect() or a drain(), from an
                // error listener as well, or a recovery that gave up: nothing more comes on it. A read still waiting
                // fails the same way on its own; this is the exit of a request whose wake-up fired, since the close
                // also reset the rejection latch below: woken by a rejection the close followed at once, it would
                // otherwise look every millisecond until its deadline (#180).
                if ($this->terminalCloses !== $closes) {
                    throw new ConnectionException('Connection is not open');
                }

                // The mux reply inbox was permission-rejected mid-flight (the async -ERR was just read);
                // no reply can arrive on it, so fail fast with the clear error rather than waiting out
                // the deadline (#167).
                if ($this->muxRejected) {
                    throw $this->muxRejectedException();
                }

                // The client dropped the mux reply inbox the request was sent on, since the server may have
                // rejected it: no reply can arrive on it.
                if ($this->muxGeneration !== $muxGeneration) {
                    throw $this->muxDroppedException();
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
                    // flag check and the await completes it immediately - no lost wakeup. The wake-up
                    // ends this wait too (#180): the inbox can be dropped under a request parked here,
                    // by the dispatch of that other read, and the slot is released only when it ends.
                    try {
                        awaitFirst([$deferred->getFuture(), $this->readSlotReleased->getFuture()], $parkedWith);
                    } catch (CancelledException) {
                        // Deadline, external cancellation or the wake-up while parked; the top-of-loop
                        // checks return the reply delivered in the same tick or throw.
                    }

                    continue;
                }

                try {
                    $read = $this->operationRead($waitCancellation, null, false, $replyArrived->getCancellation())->await();
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
     * While a reconnect is in flight it first waits for it within the total timeout
     * ({@see NatsOptions::$waitForReconnect}); a budget that runs out before the request could be
     * sent surfaces as TimeoutException rather than an empty collection.
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
        $caller = \Fiber::getCurrent();

        return async(function () use ($subject, $payload, $headers, $maxResponses, $totalTimeoutMs, $stallMs, $cancellation, $caller): array {
            // Cached: request targets repeat (unlike the per-request inbox, which is validated
            // uncached as publish()'s replyTo).
            $this->validateSubjectCached($subject);

            if ($maxResponses !== null && $maxResponses < 1) {
                throw new \InvalidArgumentException('maxResponses must be at least 1 when provided');
            }
            if ($stallMs !== null && $stallMs <= 0) {
                throw new \InvalidArgumentException('stallMs must be greater than zero when provided');
            }

            return $this->requestManyInternal($subject, $payload, $headers, $maxResponses, $totalTimeoutMs, $stallMs, $cancellation, $caller);
        });
    }

    /**
     * Executes the scatter-gather collection loop.
     *
     * The total timeout is one budget started here, shared by the wait for an in-flight reconnect, the
     * mux inbox set-up, the publish and the collection.
     *
     * @param array<string,string>|null $headers
     * @param ?\Fiber<mixed, mixed, mixed, mixed> $caller The fiber that called requestMany().
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
        ?\Fiber $caller = null,
    ): array {
        $totalMs = $totalTimeoutMs ?? $this->options->requestTimeoutMs;
        if ($totalMs <= 0) {
            throw new TimeoutException('Request timeout must be greater than zero');
        }

        $deadline = $this->monotonicSeconds() + $totalMs / 1000;
        $totalCancellation = new TimeoutCancellation($totalMs / 1000);
        $waitCancellation = $cancellation === null
            ? $totalCancellation
            : new CompositeCancellation($cancellation, $totalCancellation);

        $muxGeneration = $this->prepareRequest($subject, $waitCancellation, $cancellation, $caller);
        // As in requestInternal(): a terminal close while replies are awaited ends the collection at its next look.
        $closes = $this->terminalCloses;
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
        // Rotated with the tick, for the read: the read in flight ends without reading at each delivery, wherever it
        // waits then, as request()'s does on its reply (#174). The one registered with the waiter is the current one,
        // which the connection fires when it drops or rejects the inbox (#180): the checks below find that out.
        $replyWake = new DeferredCancellation();

        // Registered by token on the shared mux inbox instead of a fresh per-request SUB (#118); the
        // collector body (incl. the #160 cap and #135 tick rotation) is unchanged.
        $this->registerMuxWaiter($token, function (NatsMessage $message) use (&$messages, &$lastAt, &$noResponders, &$replyTick, &$replyWake, $maxResponses, $token): void {
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

            $wake = $replyWake;
            $replyWake = new DeferredCancellation();
            $this->renewMuxWake($token, $replyWake);
            $wake->cancel();
        }, $replyWake);

        try {
            $this->publishRequestWithin($subject, $payload, $headers, $replyTo, $waitCancellation, $cancellation, $caller, $closes, $muxGeneration);

            while (true) {
                // The connection closed for good while replies were awaited (see requestInternal()): what was
                // collected is returned, as below for a rejection, and nothing collected fails with the closed
                // connection. The exit of a collection whose wake-up fired right before a close that reset the
                // rejection latch (#180).
                if ($this->terminalCloses !== $closes) {
                    if ($messages !== []) {
                        break;
                    }

                    throw new ConnectionException('Connection is not open');
                }

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

                // The client dropped the mux reply inbox, since the server may have rejected it (a reconnect's
                // replay of it, once replies were coming in): likewise no further reply can arrive.
                if ($this->muxGeneration !== $muxGeneration) {
                    if ($messages !== []) {
                        break;
                    }

                    throw $this->muxDroppedException();
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
                    // slot freeing, bounded by the same slice so stall/total still fire (#135), and
                    // ended by the current wake-up as a read is, should the inbox be dropped under a
                    // collection parked here (#180).
                    try {
                        awaitFirst(
                            [$replyTick->getFuture(), $this->readSlotReleased->getFuture()],
                            new CompositeCancellation($sliceCancellation, $replyWake->getCancellation()),
                        );
                    } catch (CancelledException) {
                        // Slice/total deadline or the wake-up while parked; loop re-evaluates at the top.
                    }

                    continue;
                }

                try {
                    $read = $this->operationRead($sliceCancellation, null, false, $replyWake->getCancellation())->await();
                } catch (CancelledException $e) {
                    if ($cancellation !== null && $cancellation->isRequested()) {
                        throw $e;
                    }

                    // Slice or total deadline fired during the read; loop to re-evaluate the
                    // termination conditions (stall/total) at the top.
                    continue;
                } catch (ConnectionException $e) {
                    // The read failed with the connection going: lost, or failed over from a server in lame duck
                    // mode, with waiting for a reconnect disabled (#178, #191), closed with reconnect off, or ended by
                    // a reconnect that gave up or by a fatal -ERR. What was collected is returned, as for a close
                    // above; nothing collected fails with it. A failure on an open connection still fails the
                    // collection: a full queue, a handler's own, or a fatal -ERR whose reconnect has reopened it.
                    if ($messages === [] || $this->state === ConnectionState::Open) {
                        throw $e;
                    }

                    break;
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
     * Dials $dsn so that a close stops the dial ({@see setCloseIntent()}), when the transport can stop it: a
     * close no longer leaves a reconnect or a connect() dialling after it returned - with Amp's retry pauses,
     * some 6 s against a refused port. A transport that cannot stop its dial is dialled as before, and the
     * close waits for the dial to end ({@see awaitStoppedDials()}).
     */
    private function dialTransport(string $dsn): void
    {
        $transport = $this->transport;
        if (!$transport instanceof CancellableDialTransportInterface) {
            $transport->connect($dsn, $this->options->connectTimeoutMs)->await();

            return;
        }

        $stop = new DeferredCancellation();
        $this->dialStop = $stop;
        try {
            $transport->connect($dsn, $this->options->connectTimeoutMs, $stop->getCancellation())->await();
        } finally {
            $this->dialStop = null;
        }
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
        $this->connectionGeneration++;
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
        // The UNSUBs owed by guarded inboxes treated as rejected on the connection this one replaces (#175) are
        // moot: the server dropped whatever it held with that connection, and a replayed inbox the new server
        // rejects is released by the replay itself ({@see finishReplayWindow()}).
        $this->guardedSidsToRelease = [];

        $server = $this->nextServer();
        $this->connectedServer = $server;
        $urlCredentials = $this->extractUrlCredentials($server);
        $dsn = $this->normalizeDsn($server);
        $this->dialTransport($dsn);

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
        // its handler; an -ERR here surfaces as a connect failure the caller's policy then handles.
        if ($trailingFrames !== []) {
            // A full queue - a subscription's or a SubscriptionQueue's - or a handler that throws is reported,
            // not a failed attempt, as in the replay poll and the delivery after a reconnect (#144). Any other
            // failure still fails it, an -ERR the server keeps the connection open for included, which the
            // replay poll reports instead: a conforming server sends nothing it could reject ahead of the PONG.
            $dispatchError = null;
            try {
                $this->dispatchFrames($trailingFrames, reportOverflows: true);
            } catch (\Throwable $e) {
                $dispatchError = $e;
            }

            $this->deliverReportingFailures();

            if ($dispatchError !== null) {
                throw $dispatchError;
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
     * A reconnect this call runs announces the new connection once it is over ({@see announceOpen()}): it logs
     * the open and calls the listener before this returns, unless another Connected or Reconnected listener
     * call is running, when the listener is called from the event loop instead. The listener may close the
     * connection, or start another reconnect through what it calls, and a drain() that waited for the
     * reconnect may begin meanwhile, so a caller that goes on using the connection checks the state again
     * rather than assume it is Open, or Closed.
     *
     * @param bool $ownedByConnect True only for the hand-off from {@see performConnect()}, which
     *                             runs inside the connect fiber while {@see $connecting} is set and
     *                             must bypass the in-flight-connect guard below.
     * @param Cancellation|null $joinCancellation Bounds only a JOIN of a recovery another fiber already
     *                             runs: when it fires the joiner gets CancelledException while the
     *                             recovery carries on. A recovery this call starts runs inline in the
     *                             calling fiber and is not bounded by it: a caller that must keep to a
     *                             budget of its own starts the recovery in a fiber of its own instead and
     *                             waits for that ({@see recoverAfterFailedWrite()},
     *                             {@see recoverAfterFailedOperationRead()}, {@see startLameDuckFailover()}).
     * @param int|null $failedGeneration The {@see $connectionGeneration} the failed read or write ran on;
     *                             a failure from a connection since replaced starts no recovery.
     * @param \Throwable|null $cause The error that ended the connection. With reconnect off it is chained to
     *                             the "Reconnect is disabled" error, so that the caller, and any operation
     *                             that joins the recovery, learns why the connection ended (#172).
     */
    private function recoverConnection(
        bool $ownedByConnect = false,
        ?Cancellation $joinCancellation = null,
        ?int $failedGeneration = null,
        ?\Throwable $cause = null,
    ): void {
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

            $inProgress->getFuture()->await($joinCancellation);

            return;
        }

        // The failure came from a connection that has since been replaced - the application closed and
        // reopened it while this read or write was suspended, or while its error listener ran: the new
        // connection is healthy, and recovering it would tear it down for nothing.
        if ($failedGeneration !== null && $failedGeneration !== $this->connectionGeneration) {
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
            $opened = $this->performRecovery($cause);
            $deferred->complete();
        } catch (\Throwable $e) {
            $deferred->error($e);

            throw $e;
        } finally {
            $this->recoveryFiber = null;
            $this->reconnecting = null;
            // After $deferred settled above, so that its waiters get the reconnect's outcome before they are woken.
            $wakes = $this->wakesAfterRecovery;
            $this->wakesAfterRecovery = [];
            foreach ($wakes as $wake) {
                $wake->cancel();
            }
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

        // Announced once the reconnect is over, as performConnect() announces a connect (#145) and as the
        // delivery below runs: what the listener calls then runs as it would anywhere else. Announced inside,
        // an operation that found the new connection gone already joined this reconnect, which was waiting for
        // the listener to return: the operation waited out its timeout, or for good, and the connection stayed
        // Open on the dead socket. Now that operation reconnects again.
        if ($opened !== null) {
            // A failed initial connect handed off here: settled before its Connected listener runs, as
            // performConnect() settles a direct one, so that a connect() which joined the dial, and which the
            // listener may wait for, is not waiting for the listener in turn.
            if ($ownedByConnect) {
                $this->settleConnecting(null);
            }

            $this->announceOpen($opened);
        }

        // A reconnect that a close stopped returns without reopening the connection, and the listener may have
        // closed it since - or still be closing it, a disconnect() whose socket close takes a while - or what it
        // called given up on it. What is queued is then the close's: drain() delivers it within its budget, and
        // disconnect() discards it. Delivered here as well, it reached handlers after disconnect(), or beside
        // drain()'s own delivery and past the rules that one keeps. When what the listener called started
        // another reconnect instead, what is queued waits for that one.
        if ($this->closing || $this->state !== ConnectionState::Open) {
            return;
        }

        // Deliver any messages buffered during subscription replay now that recovery has finished and
        // we are OUT of the critical section: `reconnecting` is cleared, so a callback that publishes
        // and hits a write failure starts a fresh recovery instead of deadlocking on the in-progress
        // one, and the per-sid dispatch guard keeps it non-reentrant. (Only reached when the recovery
        // reopened the connection; the catch above rethrows on failure.) A full SubscriptionQueue and a
        // handler that throws are reported without cutting the delivery short: the read of an operation
        // that ran this reconnect may wait for a message behind them, and it delivers nothing an earlier
        // delivery left queued, so a delivery that stopped there left that operation waiting for the server's
        // next bytes (#173).
        try {
            $this->deliverReportingFailures();
        } catch (\Throwable $handlerError) {
            // Recovery itself already succeeded; only a handler(-triggered) failure can escape this
            // drain. It must not reach the recovery callers, whose catch blocks treat anything thrown
            // here as a FAILED recovery - the heartbeat paths would flip a healthy connection to
            // Closed without a Closed event or state release, and publish()'s retry would surface an
            // unrelated exception for a frame that was never written (#144). Report it as an async
            // error instead (nats.go parity: handler errors during post-reconnect delivery are
            // reported, not fatal) - safely, so a throwing user logger cannot re-open that escape.
            $this->emitErrorSafely($handlerError);
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
            // Cut short, like the reconnect backoff, by a disconnect() issued meanwhile - which then wins.
            $this->waitOutReconnectBackoff($this->backoffDelayMs($attempt));
            $this->abandonConnectIfClosed();

            try {
                $this->transport->close()->await();
            } catch (\Throwable) {
                // Ignore close failures between attempts.
            }

            try {
                $this->connectOnce();
            } catch (AuthenticationException $e) {
                $this->abandonConnectIfClosed($e);

                $this->markClosedForGood();
                $this->closeTransportBestEffort();
                $this->settleConnecting($e);
                $this->emitEvent(ConnectionEvent::Closed, $e);

                throw $e;
            } catch (\Throwable) {
                // Keep retrying until attempts are exhausted (a close is noticed after the backoff).
                continue;
            }

            $this->abandonConnectIfClosed();

            $this->markConnectionOpen();
            // Settle the in-flight connect() before the listener runs: this retry loop is still inside
            // performConnect() (the deferred is set), so a pending $connecting under the Connected/Closed
            // listener would re-open the join deadlock (#145).
            $this->settleConnecting(null);
            $this->emitEvent(ConnectionEvent::Connected);

            return true;
        }

        // Attempts exhausted: the last attempt's socket may still be open (connectOnce() dials
        // before the handshake can fail) - release it before reporting failure (#133).
        $this->closeTransportBestEffort();
        // A disconnect() issued during the last attempt, or during that close, wins like one issued
        // earlier: the retries did not fail, they were stopped, and performConnect() must not announce
        // a close disconnect() announces.
        $this->abandonConnectIfClosed();

        return false;
    }

    /**
     * Performs the actual reconnect + subscription replay, serialized by {@see recoverConnection()}.
     *
     * @param \Throwable|null $cause The error that ended the connection, chained to "Reconnect is disabled".
     * @return ConnectionEvent|null The event that announces the reopened connection, which
     *         {@see recoverConnection()} emits once the reconnect is over; null when a close stopped the
     *         reconnect.
     */
    private function performRecovery(?\Throwable $cause = null): ?ConnectionEvent
    {
        // User close-intent set before/while recovery began: do not re-open (#84).
        if ($this->closing) {
            $this->state = ConnectionState::Closed;

            return null;
        }

        if (!$this->options->reconnectEnabled) {
            $this->markClosedForGood();
            // Terminal close: same invariant as the exhaustion/auth paths - release the socket and
            // runtime state so a later manual connect() starts clean (#127/#133, missed here: #146).
            $this->closeTransportBestEffort();
            // Name any parsed inbound backlog being discarded, mirroring the outbound path (#123/#158).
            $this->reportDiscardedInboundBacklog();
            $this->releaseRuntimeState();
            // With the cause, so that the connection listener and the log learn why the connection ended even
            // when nobody waits for this recovery, as when the heartbeat started it (#172).
            $this->emitEvent(ConnectionEvent::Closed, $cause);
            // The message stays as it was, for code that matches it; the cause says why (#172).
            throw new ConnectionException('Reconnect is disabled', 0, $cause);
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
        // How the reopened connection will be announced: the first-ever open reached through the recovery loop
        // (a failed initial connect) as Connected, not Reconnected, so listeners keyed on Connected still fire
        // (#161).
        $opened = $firstConnect ? ConnectionEvent::Connected : ConnectionEvent::Reconnected;
        $unannounced = $this->unannouncedOpen;
        if ($unannounced !== null && $unannounced['generation'] === $this->connectionGeneration) {
            // The listener has not heard yet that the connection now lost was open (announceOpen() had yet to
            // call it), so for the listener the outage it last heard of goes on: it is not told of this loss
            // either, and hears of the next open as it would have heard of that one. The log records both.
            $this->unannouncedOpen = null;
            $this->logLifecycleEvent(ConnectionEvent::Disconnected, null);
            $opened = $unannounced['event'];
        } elseif (!$firstConnect) {
            $this->emitEvent(ConnectionEvent::Disconnected);
        }

        $maxAttempts = max(1, $this->options->maxReconnectAttempts);
        $lastError = null;

        for ($attempt = 1; $attempt <= $maxAttempts; $attempt++) {
            // disconnect()/drain() may have been called between attempts: stop reopening (#84).
            if ($this->closing) {
                $this->state = ConnectionState::Closed;

                return null;
            }

            $this->closeTransportBestEffort();

            // A close that came while the previous socket was closing: do not dial again.
            if ($this->closing) {
                $this->state = ConnectionState::Closed;

                return null;
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

                    return null;
                }

                // The replay window: state stays Connecting through the subscription replay and
                // the buffered-publish flush, so a concurrent publish keeps buffering (and flushes
                // in order below) instead of jumping the queue on the wire, and a failed leg falls
                // to the catch with no Open state / armed ping timer on a dead socket (#148).
                $replayed = $this->resubscribeAll();
                // A failed initial connect recovering here is not a reconnect - do not bump the
                // reconnect count for the first-ever open (#161).
                if (!$firstConnect) {
                    $this->reconnectCount++;
                }
                $this->finishReplayWindow($replayed);

                // Re-check close-intent after the replay's suspension points: flipping Open here
                // would resurrect a connection disconnect()/drain() just closed (#84).
                if ($this->closing) {
                    try {
                        $this->transport->close()->await();
                    } catch (\Throwable) {
                        // Already gone; the Closed state below is what matters.
                    }
                    $this->state = ConnectionState::Closed;

                    return null;
                }

                $this->markConnectionOpen();

                // recoverConnection() announces it once the reconnect is over.
                return $opened;
            } catch (AuthenticationException $e) {
                // disconnect()/drain() stopped this reconnect while it was authenticating: end like the
                // other stopped exits - the close is theirs to report and announce (#84).
                if ($this->closing) {
                    $this->state = ConnectionState::Closed;
                    $this->closeTransportBestEffort();

                    return null;
                }

                // Credentials will not become valid by retrying: stop the reconnect loop immediately
                // rather than hammering the server until attempts are exhausted (#46).
                $this->markClosedForGood();
                $this->closeTransportBestEffort();
                // Publishes buffered during the outage reported success: discarding them must be loud,
                // as when the attempts run out (#123).
                $this->reportDiscardedReconnectBuffer('Reconnect failed authentication');
                // Name any parsed inbound backlog being discarded, mirroring the outbound path (#123/#158).
                $this->reportDiscardedInboundBacklog();
                $this->releaseRuntimeState();
                // emitErrorSafely: a throwing user logger must not replace the error the callers get,
                // nor skip the Closed event (#158).
                $this->emitErrorSafely($e);
                $this->emitEvent(ConnectionEvent::Closed, $e);

                throw $e;
            } catch (\Throwable $e) {
                $lastError = $e;
                // disconnect()/drain() closed the connection during this attempt: stop now (below)
                // instead of backing off first.
                if ($this->closing) {
                    break;
                }

                $delayMs = $this->backoffDelayMs($attempt);
                try {
                    $this->logger->warning(
                        sprintf('NATS reconnect attempt %d/%d failed; retrying in %dms', $attempt, $maxAttempts, $delayMs),
                        ['attempt' => $attempt, 'maxAttempts' => $maxAttempts, 'delayMs' => $delayMs, 'exception' => $e],
                    );
                } catch (\Throwable) {
                    // A throwing user logger must not end the reconnect: it left the connection
                    // Connecting for good, with every operation failing and nothing left to recover it.
                }
                $this->waitOutReconnectBackoff($delayMs);
            }
        }

        // Stopped by disconnect()/drain() - during the last attempt or its backoff too: the close is
        // theirs to report and announce, so this is not an exhaustion (#84).
        if ($this->closing) {
            $this->state = ConnectionState::Closed;

            return null;
        }

        $this->markClosedForGood();

        // Publishes buffered during the outage already reported success to their callers;
        // abandoning them must be loud, and the buffer must not survive into a later manual
        // connect() where a future recovery would replay frames from this dead epoch (#123).
        // Reported safely: a throwing user logger must not skip the cleanup below (#158).
        $this->reportDiscardedReconnectBuffer('Reconnect exhausted');
        $this->reconnectBuffer = '';

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
     * The backoff between two connect attempts - a reconnect's, or retryInitialConnect()'s - cut short by
     * close-intent ({@see setCloseIntent()}).
     */
    private function waitOutReconnectBackoff(int $delayMs): void
    {
        $backoff = new DeferredCancellation();
        $this->reconnectBackoff = $backoff;

        try {
            // Close-intent that arrived before this backoff could be cut short (while the logger ran,
            // say) must not cost the whole delay either.
            if ($this->closing) {
                return;
            }

            delay($delayMs / 1000, cancellation: $backoff->getCancellation());
        } catch (CancelledException) {
            // disconnect()/drain(): the reconnect loop stops at its next close-intent check.
        } finally {
            $this->reconnectBackoff = null;
        }
    }

    /**
     * Ends a reconnect's replay window: flushes the publishes buffered during the outage, and tells the
     * new connection what the client changed about the replayed subscriptions meanwhile. unsubscribe() -
     * and a drainSubscription() that cannot wait for the reconnect - only removes a subscription locally
     * while the connection is not open, and unsubscribe() with a max only records it, while
     * resubscribeAll() had already re-subscribed the sid: left alone, the server would keep delivering
     * to a sid nothing handles any more, or past the max - for a queue group, taking a share of the
     * group's messages. Repeats until a pass writes nothing, so the connection flips Open with nothing
     * left unsent: a publish buffered while one of these writes was under way would otherwise stay in
     * the buffer - reported as sent, and sent one outage later, after newer frames, if ever.
     *
     * @param array<int, array{max: ?int, received: int}> $replayed The sids resubscribeAll() re-subscribed,
     *        with the auto-unsubscribe max each was replayed with and the messages received before that.
     */
    private function finishReplayWindow(array $replayed): void
    {
        while (true) {
            $this->flushReconnectBuffer();

            $frames = '';
            foreach ($replayed as $sid => $replay) {
                if (!isset($this->subscriptionMeta[$sid])) {
                    $frames .= $this->codec->encodeUnsubscribe($sid);
                    unset($replayed[$sid]);
                    // This UNSUB is the one a guarded inbox the replay's drain treated as rejected owes (#175): its
                    // operation's unsubscribe() then has nothing left to write.
                    $this->takeOwedRelease($sid);

                    continue;
                }

                $max = $this->autoUnsubMax[$sid] ?? null;
                if ($max !== null && $max !== $replay['max']) {
                    // The replayed SUB counts from zero, like resubscribeAll()'s own re-arm.
                    $remaining = $max - $replay['received'];
                    $frames .= $this->codec->encodeUnsubscribe($sid, $remaining > 0 ? $remaining : null);
                    $replayed[$sid]['max'] = $max;
                }
            }

            if ($frames === '') {
                // Nothing written since the buffer was flushed, so nothing can have been buffered since.
                return;
            }

            $this->transport->write($frames)->await();
        }
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
     * immediately followed by its UNSUB re-arm, in registration order - except that the mux
     * reply-inbox SUB and each guarded inbox's SUB ({@see subscribeGuarded()}, #175) are followed by a PING,
     * which confirms them: until its PONG a 'maximum subscriptions exceeded' -ERR the drain below reads drops
     * the mux and rejects the guarded inbox, its operation told, as on a first subscribe.
     *
     * @return array<int, array{max: ?int, received: int}> The sids re-subscribed, with the auto-unsubscribe
     *         max each was replayed with and the messages it had received by then.
     *
     * @phpstan-impure Writes the replay and reads the server's answers, suspending meanwhile: close-intent can
     *                 be set, and subscriptions dropped, while it runs.
     */
    private function resubscribeAll(): array
    {
        $buffer = '';
        $replayed = [];

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
            $replayed[$sid] = ['max' => $max, 'received' => $this->receivedCounts[$sid] ?? 0];

            if ($remaining !== null) {
                // A fresh SUB resets the server's per-sid count, so re-arm auto-unsubscribe with the
                // REMAINING allowance; the cumulative local counter still ends delivery at the
                // original max (#112). Mirrors nats.go resendSubscriptions.
                $buffer .= $this->codec->encodeUnsubscribe($sid, $remaining);
            }

            if ($sid === $this->muxSid) {
                // The new server may reject the mux like any replayed SUB, so it is unconfirmed until the PONG of
                // this PING. A subscription limit rejects every SUB from the first one it refuses, so an -ERR for
                // the limit ahead of that PONG means the mux is refused too, while those of the SUBs replayed
                // after it come behind the PONG: a mux that keeps its slot is not dropped for them. The queue
                // holds only this replay's slots - connectOnce() cleared it, and nothing else writes a PING until
                // the connection is Open - in the order of their PINGs, so each PONG is its own PING's.
                $this->muxConfirmed = false;
                $this->muxFence = $this->enqueuePongSlot();
                $buffer .= $this->codec->encodePing();
            } elseif (isset($this->guardedSids[$sid])) {
                // A guarded inbox gets the same rule (#175): unconfirmed again until the PONG of the PING replayed
                // behind its SUB, and rejected, its operation told, by a limit -ERR ahead of that PONG, which the
                // drain below reads. The slot replaced here is the old connection's, failed by connectOnce().
                $this->unconfirmedSids[$sid] = $this->enqueuePongSlot();
                $buffer .= $this->codec->encodePing();
            }
        }

        // Nothing to replay (no subscriptions, or all were dropped above): no write, no drain.
        if ($buffer === '') {
            return [];
        }

        // A single large buffer is fine here: write() runs inline and suspends on backpressure
        // (#136) - the same path flushReconnectBuffer() takes.
        $this->transport->write($buffer)->await();

        // One bounded poll for the whole replay, so that the server's prompt answers to it are seen: an -ERR
        // that ends the connection aborts this reconnect attempt, and one the server keeps the connection open
        // for, such as a rejected SUB, is reported (#137 keeps the detection, drops the per-sid latency floor).
        $this->drainImmediateServerFrames();

        return $replayed;
    }

    /**
     * Polls for any immediate frames emitted by the server after a protocol write.
     *
     * This is primarily used during reconnect subscription replay, so that a prompt `-ERR` is seen: one
     * that ends the connection fails the attempt, and one the server keeps the connection open for, such as
     * a rejected SUB, is reported.
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
     * user or heartbeat read can start against the new socket until the recovery flips Open. (A read
     * whose failure starts the recovery releases the slot first, so a read issued from a listener or a
     * handler during the recovery is state-gated like any other.)
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

            // Per-frame containment (#128): a prompt -ERR that ends the connection still aborts this reconnect
            // attempt (rethrown after the loop), but sibling MSG frames from the same chunk
            // are enqueued first instead of being discarded. handleFrame() ignores +OK frames.
            // A message for a subscription whose queue is full (SlowConsumerPolicy::Error) is dropped
            // and reported instead of failing the attempt: nothing delivers while the reconnect runs,
            // so every attempt would fail the same way until the reconnect gave up and closed the
            // connection - for a subscriber that merely could not keep up.
            try {
                $this->dispatchFrames($this->parser->push($chunk), reportOverflows: true);
            } catch (ProtocolException $parseError) {
                // A mid-chunk parse failure fails this attempt, and the retry's connectOnce()
                // replaces the parser - which would drop the frames it retained (#147). Enqueue
                // them first (the post-recovery drainAllPending() delivers them), then rethrow so
                // attempt-failure semantics stay unchanged.
                try {
                    $this->dispatchFrames($this->parser->takeParsedFrames(), reportOverflows: true);
                } catch (\Throwable $recoveredFailure) {
                    // The rethrow below already fails this attempt; dispatchFrames() enqueued the
                    // recovered MSG frames per frame before rethrowing (#128). What they raised is reported,
                    // as below, so that the server's rejection of a replayed SUB is not lost with the attempt.
                    $this->emitErrorSafely($recoveredFailure);
                }

                throw $parseError;
            } catch (\Throwable $frameFailure) {
                // A failure that does not end the connection does not fail the attempt either: the server
                // rejecting a replayed SUB beyond the maximum subscriptions would otherwise fail every
                // attempt the same way, until the reconnect gave up and closed the connection. Reported
                // instead, as the overflow above is.
                if ($this->frameFailureEndsConnection($frameFailure)) {
                    throw $frameFailure;
                }

                $this->emitErrorSafely($frameFailure);
            }
        }
    }

    /**
     * Computes reconnect delay with exponential backoff, capped at reconnectMaxDelayMs.
     */
    private function backoffDelayMs(int $attempt): int
    {
        $base = max(1, $this->options->reconnectDelayMs);
        // Capped BEFORE the int cast: base * 2^(attempt - 1) leaves the int range after ~60 attempts
        // (PHP turns it into a float, INF eventually), and casting an out-of-range float yields a
        // garbage, possibly negative, delay that delay() rejects - a long reconnect loop crashed
        // instead of backing off at the cap.
        $capped = (int) min($base * (2 ** ($attempt - 1)), max($base, $this->options->reconnectMaxDelayMs));
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
     * The slot is queued in the same synchronous step that hands its PING to the transport, which
     * takes writes in the order they are made, so queue order matches the order the PINGs reach the
     * wire even when several fibers send concurrently. A PING written from a fiber of its own has its
     * slot queued by that fiber ({@see writeBounded()}): queued before that fiber ran, a PING another
     * fiber wrote meanwhile - the one behind the reply inbox's SUB, say - reached the wire first, and
     * its PONG completed this slot.
     *
     * @return DeferredFuture<null>
     */
    private function enqueuePongSlot(): DeferredFuture
    {
        $slot = $this->newPongSlot();
        $this->pongWaiters[] = $slot;

        return $slot;
    }

    /**
     * A pong-correlation slot that is not queued yet, for a PING that {@see writeBounded()} writes and queues
     * the slot for.
     *
     * @return DeferredFuture<null>
     */
    private function newPongSlot(): DeferredFuture
    {
        /** @var DeferredFuture<null> $slot */
        $slot = new DeferredFuture();
        // Slots without a live waiter (heartbeat placeholders, timed-out flushes) are errored at
        // epoch teardown; suppress unhandled-error reporting - waiters still get the error from
        // their own await().
        $slot->getFuture()->ignore();

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
     *
     * @param list<array{\Throwable, string}> $reports Collects what the frame reports, with its log level,
     *        for {@see dispatchFrames()} to report once the whole chunk is queued.
     * @param bool $stale Whether the frame was read on a connection that is gone, replaced or ended while an
     *        earlier frame of its chunk was handled ({@see handleStaleFrame()}, #182), or that a lame-duck failover
     *        started in a fiber of its own is leaving (#191). Only a MSG, HMSG or +OK still comes here then, or a
     *        PONG of a connection only being left, and a delivery on the reply inbox is queued without confirming
     *        the replayed inbox.
     * @param bool $failOverInItsOwnFiber For an INFO: whether a lame-duck failover it brings starts in a fiber of its
     *        own ({@see handleServerInfoUpdate()}, {@see dispatchFrames()}).
     * @param-out list<array{\Throwable, string}> $reports
     */
    private function handleFrame(ProtocolFrame $frame, array &$reports, bool $stale = false, bool $failOverInItsOwnFiber = false): void
    {
        if ($frame->type === ProtocolFrameType::Ping) {
            $drainDeadline = $this->drainDeadline;
            if ($this->state === ConnectionState::Draining && $drainDeadline !== null) {
                // Bounded like every other write of drain() (#149): a PONG stuck behind a stalled socket
                // must not hold the drain past its budget. Past it, drain()'s teardown closes the socket.
                try {
                    $this->writeBounded($this->codec->encodePong(), $this->remainingBudgetCancellation($drainDeadline));
                } catch (CancelledException) {
                    // Wedged: see above.
                } catch (\Throwable $pongError) {
                    // The socket is gone, and with it the PONG the drain's flush waits for: end that flush now,
                    // whichever fiber's read met this PING, as a read failing during a drain does. Not marked as
                    // ending the connection, which the drain closes anyway, so the read treats it like any other
                    // failure: it reports it when it reports failures (the drain's own read, a serving loop's),
                    // and throws it otherwise.
                    $this->failPongWaiters(new ConnectionException('Connection lost before the server answered the PING'));

                    throw $pongError;
                }

                return;
            }

            try {
                $this->transport->write($this->codec->encodePong())->await();
            } catch (\Throwable $pongError) {
                // A socket that will not take the PONG is gone (#171).
                $this->connectionEndingFailures[$pongError] = true;

                throw $pongError;
            }

            return;
        }

        if ($frame->type === ProtocolFrameType::Pong) {
            // ANY pong proves the server is alive, so the watchdog resets unconditionally; flush
            // completion is per-PING (#117): the oldest queued slot is the PING this PONG answers
            // (TCP preserves order), so a stale heartbeat PONG completes the heartbeat's
            // placeholder - never a later flush()/drain() waiter's slot.
            $this->outstandingPings = 0;

            $slot = array_shift($this->pongWaiters);
            if ($slot !== null && $slot === $this->muxFence) {
                // The server answered the PING written behind the mux SUB without rejecting the SUB first.
                $this->muxFence = null;
                $this->confirmMux();
            }

            if ($slot !== null) {
                // The same for the PING written behind a guarded inbox's SUB (#175): its PONG confirms that inbox.
                $this->confirmSubscriptionFencedBy($slot);
            }

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
                $reports[] = [new NatsException('Discarding malformed async INFO frame: ' . $e->getMessage()), 'error'];

                return;
            }
            $this->handleServerInfoUpdate($failOverInItsOwnFiber);

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
                // substring match unambiguous. ensureMuxInbox() records the mux before it writes the SUB,
                // so this holds whichever read meets the -ERR, the replay of a reconnect included. The
                // requests waiting on the inbox are woken once the latch is set, so that each fails at its
                // next look, wherever its read waits (#180).
                if ($this->muxSid !== null && $this->muxBase !== null && str_contains($error, $this->muxBase)) {
                    // dropSubscriptionState() also clears the sid's slow-consumer exemption flag.
                    $this->dropSubscriptionState($this->muxSid);
                    $this->forgetMux();
                    $this->muxInboxSetup = null;
                    $this->muxRejected = true;
                    $this->wakeMuxWaiters();
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
                            // A guarded inbox the server has rejected by name has nothing left to confirm or to
                            // take for the limit's (#175); its owner fails and unsubscribes it.
                            unset($this->unconfirmedSids[$rejectedSid]);
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
                $reports[] = [new NatsException('Server sent recoverable error frame: ' . $error), 'error'];

                return;
            }

            if ($this->isSubscriptionLimitError($error)) {
                // The -ERR names no subject: it rejects the SUB whose answer the server owed next, which is any SUB
                // written with a PING behind it whose PONG has not arrived: the reply inbox's, and each guarded
                // inbox's (#175). Both are dropped before the read fails or reports, so that whichever fiber's read
                // met the -ERR, the operations waiting on them fail at their next look rather than at their deadlines.
                $muxSid = $this->muxSid;
                if ($muxSid !== null && !$this->muxConfirmed) {
                    $this->dropUnconfirmedMux($muxSid);
                }

                $this->rejectUnconfirmedSubscriptions($error);
            }

            $serverError = new ConnectionException('Server sent error frame: ' . $error);
            // The read fails with it either way, as it always did; only an -ERR the server closes the
            // connection after ends the connection here (#171).
            if (!$this->isServerErrorKeepingTheConnectionOpen($error)) {
                $this->connectionEndingFailures[$serverError] = true;
            }

            throw $serverError;
        }

        if ($frame->type === ProtocolFrameType::Msg || $frame->type === ProtocolFrameType::HMsg) {
            $sid = $frame->sid;
            if ($sid === null || !isset($this->subscriptions[$sid])) {
                return;
            }

            if ($sid === $this->muxSid && !$stale) {
                // A delivery on the mux proves the server holds it, as its fence PONG does. Recorded here, so
                // that an -ERR later in the same chunk finds it. Not a delivery of the server the connection
                // left (#182): it says nothing about the replayed mux, which the new server's fence PONG confirms.
                $this->confirmMux();
            }

            if (!$stale && isset($this->unconfirmedSids[$sid])) {
                // The same for a guarded inbox (#175): a delivery on it proves the server holds it.
                unset($this->unconfirmedSids[$sid]);
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
            $this->enqueueMessage($sid, $message, $reports);
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
     *
     * @param list<array{\Throwable, string}> $reports Collects a drop policy's report ({@see handleFrame()}).
     * @param-out list<array{\Throwable, string}> $reports
     */
    private function enqueueMessage(int $sid, NatsMessage $message, array &$reports): void
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
            // A drop policy's report logs at debug level: a routine per-message condition that must not
            // flood error logs. It is made once the chunk is queued, like every report of a read.
            if ($this->options->slowConsumerPolicy === SlowConsumerPolicy::DropOldest) {
                $queue->dequeue();
                $reports[] = [new NatsException('Slow consumer on sid ' . $sid . ': dropped oldest message'), 'debug'];
            } elseif ($this->options->slowConsumerPolicy === SlowConsumerPolicy::DropNewest) {
                $reports[] = [new NatsException('Slow consumer on sid ' . $sid . ': dropped newest message'), 'debug'];

                return;
            } else {
                // Error policy: the overflowing message is dropped (core NATS will not resend it) and
                // the loss is surfaced loudly by THROWING - the single surfacing point. dispatchFrames()
                // decides where it goes: the application's own read is thrown the first one, unless another
                // failure in the chunk outranks it, and reports any later one (#158), and a read that must
                // not fail on it - an operation's read of another subscription's overflow, a reconnect,
                // the heartbeat, a drain - reports it. An extra emitError() here would report the SAME
                // exception twice. The intake count charged above is deliberately NOT rolled back: the
                // server already counted this message toward the auto-unsub max, so it must count here too
                // or completeAutoUnsubIfSatisfied() never fires and the subscription leaks (#159/#112).
                throw new SlowConsumerException($sid);
            }
        }

        $queue->enqueue($message);
        // The queue is now non-empty: record the sid in the dirty set so drainAllPending() scans it.
        // Setting an existing key is idempotent and does NOT change its position, so the set's contents
        // stay a subset of the sids; drainAllPending() re-imposes ascending (registration) order (#162).
        $this->pendingDirty[$sid] = true;
    }

    /**
     * Delivers the queued messages. A SubscriptionQueue whose own buffer is full does not end the delivery
     * ({@see drainPendingForSid()}): with $reportOverflows its overflow is reported at once (unless it is
     * an overflow of $ownSid), and an overflow to throw is held until everything else is delivered. Then
     * the first held one is thrown and the rest are reported, the #158 rule dispatchFrames() follows. With
     * NatsOptions::$slowConsumerErrorsFailOperations the thrown one is reported as well, as a
     * SubscriptionQueue's overflow always was before that option existed: code that swallows an
     * operation's failure must not make it vanish. A handler that fails otherwise does not end the delivery
     * either (#177): its subscription's delivery stops at the failing message, the rest of that subscription
     * left queued for the next pass, which your own next read makes before it reads
     * ({@see deliverStoppedSubscriptions()}, #186), and the other subscriptions' messages are still delivered, in
     * sid order, so that an operation waiting for one of them gets it now rather than with the server's next bytes.
     * The first such failure is held and thrown once the pass is over, ahead of any held overflow, which
     * are then all reported; a second subscription that fails in the same pass is stopped the same way and
     * its failure is reported, since there is one exception to throw. With $reportHandlerFailures (a drain,
     * a read nobody awaits, a serving loop's read, an operation's read) a failure is reported at once instead
     * and that subscription's delivery goes on. A handler of $ownSid that fails is held and thrown either
     * way: it fails the operation it belongs to. Reports are made as the delivery goes, so a drain's deadline,
     * checked before each delivery, counts the time they take, and nothing restarts the pass (#149).
     */
    private function deliverPending(bool $reportOverflows, ?int $ownSid = null, bool $reportHandlerFailures = false): void
    {
        $held = [];
        $heldFailures = [];
        $failure = null;
        try {
            $this->drainAllPending($held, $heldFailures, $reportOverflows, $ownSid, $reportHandlerFailures);
        } catch (\Throwable $e) {
            $failure = $e;
        }

        $this->throwWhatThePassHeld($failure, $heldFailures, $held);
    }

    /**
     * Delivers what is queued for an operation's own subscription ahead of the operation's read ({@see readChunk()},
     * #179), when there is something to deliver and nothing else will: the sid is dirty, no delivery to it is under
     * way further up some fiber's stack (that one resumes and delivers it, in order), and the queued messages are not
     * a disconnect()'s to discard ({@see leftoversBelongToAClose()}). The one subscription is delivered with the rules
     * an operation's read delivers it with ({@see drainPendingForSid()}): its handler is the library's and never
     * suspends, except in an error listener a dropped message is reported to; its overflow and its handler's failure
     * are the operation's own, held and thrown as {@see deliverPending()} throws them. The delivery fires the
     * subscription's wake-up as any delivery does, so other reads waiting for it look again as well - this read
     * among them, since it waits with that wake-up ({@see readChunk()}'s $waitCancellation): the wake-up firing
     * here already ends this read at its next check, and the read's explicit return on a true result below is the
     * same outcome made plain, not a second mechanism.
     *
     * @return bool Whether anything was delivered: the read then returns without reading, and the operation looks.
     */
    private function deliverQueuedForOwnSid(int $ownSid, bool $reportOverflows, bool $reportHandlerFailures): bool
    {
        if (!isset($this->pendingDirty[$ownSid]) || isset($this->dispatchingSids[$ownSid]) || $this->leftoversBelongToAClose()) {
            return false;
        }

        $held = [];
        $heldFailures = [];
        $failure = null;
        $delivered = 0;
        try {
            $delivered = $this->drainPendingForSid($ownSid, $held, $heldFailures, $reportOverflows, $ownSid, $reportHandlerFailures);
        } catch (\Throwable $e) {
            $failure = $e;
        }

        $this->throwWhatThePassHeld($failure, $heldFailures, $held);

        return $delivered > 0;
    }

    /**
     * Continues, ahead of your own read ({@see readChunk()}, #186), the subscriptions whose delivery an earlier pass
     * stopped at a handler failure it held to throw ({@see $stoppedByAFailure}): the rest of each was left queued, in
     * order, for the next pass, which otherwise only a read that receives anything, or a serving loop's read, makes.
     * In sid order, whatever order they stopped in, and with the rules {@see deliverPending()} has for your own read:
     * a handler that throws stops its subscription again, the first such failure is thrown once the pass is over
     * and any other is reported, and a SubscriptionQueue's overflow is held and thrown, or reported, as
     * $reportOverflows says. The pass then goes over the subscriptions stopped while it ran, by a nested read in a
     * handler it ran that the handler caught, or by another fiber's delivery while one of its handlers awaited: the
     * read would otherwise go to the socket with their rest queued and nothing else to continue it. Each
     * subscription at most once per pass, so that the rest of one whose handler throws again here waits for the
     * next read, which throws its next failure: one failure per read.
     *
     * Only these subscriptions: the later sids of a delivery held up in another fiber's handler that awaits stay
     * queued for that delivery, unless a handler failure had already stopped one of them, which is then continued
     * whole, in order, the messages the held-up delivery brought for it included. {@see drainPendingForSid()} keeps
     * its guards: a subscription whose delivery is under way further up some fiber's stack is left to it, still
     * marked, and one that is gone, or whose queue has emptied, loses its mark. A disconnect() that begins while a
     * handler the pass runs awaits does not end the pass: it goes on to the other stopped subscriptions, as any pass
     * under way does. Only a read that would start its pass during the close leaves them to it
     * ({@see leftoversBelongToAClose()}).
     */
    private function deliverStoppedSubscriptions(bool $reportOverflows): void
    {
        $visited = [];
        $held = [];
        $heldFailures = [];
        $failure = null;
        try {
            // The marked subscriptions, then again those stopped while the pass ran - by a nested read in a handler it
            // ran, which the handler caught, or by another fiber's delivery while one of its handlers awaited - each
            // once: the read would otherwise go to the socket with their rest queued and nothing else to continue it.
            while (($stopped = array_diff_key($this->stoppedByAFailure, $visited)) !== []) {
                $sids = array_keys($stopped);
                // The set is in the order the subscriptions were first marked, a sid marked again keeping its place,
                // and two reads can stop them in either order: sorted, like the delivery of a chunk.
                sort($sids);
                foreach ($sids as $sid) {
                    $visited[$sid] = true;
                    $this->drainPendingForSid($sid, $held, $heldFailures, $reportOverflows);
                }
            }
        } catch (\Throwable $e) {
            $failure = $e;
        }

        $this->throwWhatThePassHeld($failure, $heldFailures, $held);
    }

    /**
     * Throws what a delivery pass held, once the pass is over ({@see deliverPending()},
     * {@see deliverQueuedForOwnSid()}, {@see deliverStoppedSubscriptions()}). The first handler failure held by the
     * pass is thrown; the others are reported, as the frame failures behind the one dispatchFrames() rethrows are
     * (#128). One that ended the pass outright wins. Without a failure the first held overflow is thrown and the rest
     * are reported; with NatsOptions::$slowConsumerErrorsFailOperations the thrown one is reported as well, as a
     * SubscriptionQueue's overflow always was before that option existed.
     *
     * @param \Throwable|null $failure What ended the pass outright, if anything did.
     * @param list<\Throwable> $heldFailures The handler failures the pass held ({@see drainPendingForSid()}).
     * @param list<SlowConsumerException> $held The overflows the pass held.
     */
    private function throwWhatThePassHeld(?\Throwable $failure, array $heldFailures, array $held): void
    {
        foreach ($heldFailures as $heldFailure) {
            if ($failure === null) {
                $failure = $heldFailure;

                continue;
            }

            $this->emitErrorSafely($heldFailure);
        }

        $thrownOverflow = null;
        foreach ($held as $overflow) {
            if ($failure === null && $thrownOverflow === null) {
                $thrownOverflow = $overflow;

                continue;
            }

            $this->emitErrorSafely($overflow);
        }

        if ($failure !== null) {
            throw $failure;
        }

        if ($thrownOverflow !== null) {
            if ($this->options->slowConsumerErrorsFailOperations) {
                $this->emitErrorSafely($thrownOverflow);
            }

            throw $thrownOverflow;
        }
    }

    /**
     * {@see deliverPending()} for a delivery whose failures have nobody to be thrown to - a drain's, the
     * heartbeat's, a reconnect handshake's, the one after a reconnect: every overflow and every handler failure
     * is reported, and the rest is still delivered. Nothing is thrown: only a handler can fail during a delivery.
     */
    private function deliverReportingFailures(): void
    {
        $this->deliverPending(reportOverflows: true, reportHandlerFailures: true);
    }

    /**
     * Drains all queued subscription messages in SID order.
     *
     * @param list<SlowConsumerException>|null $heldOverflows Holds the SubscriptionQueue buffer overflows to
     *        throw instead of ending the delivery on them ({@see drainPendingForSid()}).
     * @param list<\Throwable>|null $heldFailures Holds the handler failures to throw instead of ending the
     *        delivery on them ({@see drainPendingForSid()}).
     * @param-out ($heldOverflows is null ? null : list<SlowConsumerException>) $heldOverflows
     * @param-out ($heldFailures is null ? null : list<\Throwable>) $heldFailures
     */
    private function drainAllPending(
        ?array &$heldOverflows = null,
        ?array &$heldFailures = null,
        bool $reportOverflows = false,
        ?int $ownSid = null,
        bool $reportHandlerFailures = false,
    ): void {
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
            $this->drainPendingForSid(array_key_first($this->pendingDirty), $heldOverflows, $heldFailures, $reportOverflows, $ownSid, $reportHandlerFailures);

            return;
        }

        // Two or more sids have backlog: sort ascending so cross-sid delivery keeps the registration-order
        // sequencing of the old array_keys(pendingMessages) scan (subscribe() inserts sids ascending).
        $sids = array_keys($this->pendingDirty);
        sort($sids);
        foreach ($sids as $sid) {
            $this->drainPendingForSid($sid, $heldOverflows, $heldFailures, $reportOverflows, $ownSid, $reportHandlerFailures);
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
        $data = json_decode($infoPayload, true, 512, JSON_THROW_ON_ERROR);
        // Valid JSON that is not an object is as malformed as broken JSON. A number ("INFO 1") used to fail
        // the read with a TypeError in fromInfoPayload(), and an array ("INFO [1]"), which decodes to a PHP
        // array like an object does, replaced the server info with defaults. The parser has trimmed the payload.
        if (!str_starts_with($infoPayload, '{')) {
            throw new \JsonException('INFO payload is not a JSON object');
        }

        /** @var array<string,mixed> $data */
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
     * Returns true for a server -ERR the server sends while keeping the connection open: any permissions
     * violation (for a publish, a publish with a reply subject, or a subscription), a SUB beyond the maximum
     * subscriptions, and an invalid publish subject. nats-server sends these without closing the connection,
     * where it closes it after every other -ERR (Stale Connection, Authorization Violation, Maximum Payload
     * Violation, ...). The recoverable ones ({@see isRecoverableServerError()}, which also takes an invalid
     * subject) never fail a read and do not get here. One exception: when an account's subscription limit is
     * lowered below what a client holds, the server sends 'maximum subscriptions exceeded' and then closes
     * the connection, which the EOF that follows ends.
     */
    private function isServerErrorKeepingTheConnectionOpen(string $error): bool
    {
        $normalized = strtolower(trim($error, " '\t\r\n\0\x0B"));

        return str_starts_with($normalized, 'permissions violation')
            || $this->isSubscriptionLimitError($error)
            || $normalized === 'invalid publish subject';
    }

    /**
     * Returns true for the -ERR rejecting a SUB beyond the connection's subscription limit. It names neither the
     * subject nor the sid, so only the order of the server's answers can tie it to a SUB ({@see $muxConfirmed}).
     * The one classifier of that -ERR: the JetStream operations whose guarded inbox's rejection handler got its
     * text ({@see subscribeGuarded()}) ask it, through NatsClient, rather than match the text themselves.
     *
     * @internal Low-level mechanism for the JetStream reply inboxes; not general API.
     */
    public function isSubscriptionLimitError(string $error): bool
    {
        return strtolower(trim($error, " '\t\r\n\0\x0B")) === 'maximum subscriptions exceeded';
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
     *
     * @param \Throwable $cause Why the heartbeat gave up on the connection (#172).
     */
    private function recoverFromHeartbeatFailure(int $generation, \Throwable $cause): void
    {
        // The failure came from a connection since replaced (the application closed and reopened it while
        // this tick's PING write was suspended): the new connection, and its own heartbeat, are healthy.
        if ($generation !== $this->connectionGeneration) {
            return;
        }

        $this->cancelPingTimer();

        try {
            $this->recoverConnection(cause: $cause);
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

        // The connection this tick checks: its PING write and PONG read suspend, and the application may
        // close and reopen the connection meanwhile.
        $generation = $this->connectionGeneration;
        $this->outstandingPings++;

        if ($this->outstandingPings > $this->options->maxPingsOut) {
            $this->recoverFromHeartbeatFailure($generation, new ConnectionException(
                match ($this->options->maxPingsOut) {
                    0 => 'The heartbeat allows no unanswered PING (maxPingsOut is 0)',
                    1 => 'The server did not answer the last PING',
                    default => sprintf('The server did not answer the last %d PINGs', $this->options->maxPingsOut),
                },
            ));

            return;
        }

        // The heartbeat PING occupies a pong-correlation slot even though nothing awaits it
        // (#117): its PONG must consume ITS queue position - otherwise a heartbeat PONG left
        // unconsumed by the bounded self-read below would complete the next flush()/drain()
        // waiter's slot one PING early.
        $slot = $this->enqueuePongSlot();

        try {
            $this->transport->write($this->codec->encodePing())->await();
        } catch (\Throwable $pingError) {
            // The PING never hit the wire: drop its slot so correlation stays aligned (the
            // recovery below clears the rest on the epoch change anyway).
            $this->discardPongSlot($slot);
            $this->recoverFromHeartbeatFailure($generation, $pingError);

            return;
        }

        // Consume the server PONG ourselves so liveness detection does not depend on the
        // application actively calling processIncoming(). If a user read is already running,
        // it will consume the PONG instead and reset the counter.
        $this->consumeHeartbeatResponse($generation);
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
     *
     * @param int|null $generation The {@see $connectionGeneration} of the tick's PING, handed to
     *                             recoverConnection() when the read fails; null for the current connection.
     */
    private function consumeHeartbeatResponse(?int $generation = null): void
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

        $closedError = null;
        $protocolViolation = null;
        try {
            $chunk = $this->transport->readLine(new TimeoutCancellation($timeoutSeconds))->await();
        } catch (TransportClosedException $peerClosed) {
            // The peer closed the socket during the heartbeat read. Recover, but only after the
            // finally clears readInProgress (recoverConnection -> connectOnce reads the socket).
            $closedError = $peerClosed;
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
                $this->recoverConnection(failedGeneration: $generation, cause: $protocolViolation);
            } catch (\Throwable) {
                $this->state = ConnectionState::Closed;
            }

            return;
        }

        if ($closedError !== null) {
            try {
                $this->recoverConnection(failedGeneration: $generation, cause: $closedError);
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
            // them buffered until the next processIncoming(), mirroring processIncoming(). A handler
            // that throws, or a full SubscriptionQueue, is reported and the rest is still delivered:
            // there is nobody to throw either to on this path.
            $this->deliverReportingFailures();
        } catch (ProtocolException $parseError) {
            // A mid-chunk parse failure: frames parsed before it are already consumed from the
            // wire and would otherwise vanish (#147). Deliver them through the normal
            // enqueue/dispatch path and surface the failure; escalation (recovery) stays with the
            // ping watchdog / next user read, not with the event-loop timer.
            // Contained like the clean-path dispatch above: surfaced below, never thrown out of the timer -
            // and a delivery failure does not hide a dispatch failure, nor the other way round.
            try {
                $this->dispatchFrames($this->parser->takeParsedFrames());
            } catch (\Throwable $e) {
                $dispatchError = $e;
            }

            $this->deliverReportingFailures();

            // Safely: an exception thrown from this catch block would escape the timer (the catch
            // below does not cover its sibling), and a throwing user logger must not do that.
            $this->emitErrorSafely($parseError);
        } catch (\Throwable $unexpected) {
            // The dispatch and the delivery are contained above, so nothing else is expected here. Should
            // anything throw, it is reported: never thrown out of the event-loop timer.
            $this->emitErrorSafely($unexpected);
        }

        if ($dispatchError !== null) {
            // Previously a fatal frame (e.g. a server -ERR) observed during the heartbeat read was
            // swallowed whole. Surface it through the error listener (#128) - safely, since a throwing
            // user logger must not reach the event-loop timer.
            $this->emitErrorSafely($dispatchError);

            // A frame that ends the connection recovers it here as well, like a socket the peer closed
            // (#171): left Open until the next tick, the connection failed whichever operation came first.
            if ($this->frameFailureEndsConnection($dispatchError)) {
                try {
                    $this->recoverConnection(failedGeneration: $generation, cause: $dispatchError);
                } catch (\Throwable) {
                    $this->state = ConnectionState::Closed;
                }
            }
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
        if ($event === ConnectionEvent::Closed) {
            // A drain() under way must not announce this close again when it ends.
            $this->closedAnnounced = true;
        }

        $this->logLifecycleEvent($event, $error);
        $this->notifyConnectionListener($event, $error);
    }

    /**
     * Logs a lifecycle transition, whether or not a connection listener is configured (#69). Guarded like the
     * listener: a throwing user logger must neither keep the transition from the listener nor break the caller -
     * a drain() or disconnect() that is closing the connection.
     */
    private function logLifecycleEvent(ConnectionEvent $event, ?\Throwable $error): void
    {
        try {
            if ($error !== null) {
                $this->logger->warning('NATS connection ' . $event->name, ['event' => $event->name, 'exception' => $error]);
            } else {
                $this->logger->info('NATS connection ' . $event->name, ['event' => $event->name]);
            }
        } catch (\Throwable) {
            // Swallowed: see above.
        }
    }

    /**
     * Calls the configured connection listener, if any, counting the Connected and Reconnected calls while they
     * run ({@see $openListenerCalls}).
     */
    private function notifyConnectionListener(ConnectionEvent $event, ?\Throwable $error): void
    {
        $listener = $this->options->connectionListener;
        if ($listener === null) {
            return;
        }

        $announcesOpen = $event === ConnectionEvent::Connected || $event === ConnectionEvent::Reconnected;
        if ($announcesOpen) {
            $this->openListenerCalls++;
        }

        try {
            $listener($event, $error);
        } catch (\Throwable) {
            // A throwing listener must never break connection handling.
        } finally {
            if ($announcesOpen) {
                $this->openListenerCalls--;
            }
        }
    }

    /**
     * Announces a connection that a reconnect has just opened, once the reconnect is over
     * ({@see recoverConnection()}): $event is Reconnected, or Connected for the first-ever open.
     *
     * The open is logged at once, and the listener is called at once as well - unless a Connected or Reconnected
     * listener call is already running. The reconnect was then most likely run by an operation that call waits
     * for, which found the connection the call announced gone already. Called from that operation, the listener
     * would run nested inside the call that waits for it, and under a server that drops every new connection the
     * calls would nest one level deeper with every connection - a suspended fiber each, with the operation that
     * ran the first reconnect blocked until the server settles. The listener is called from the event loop
     * instead, and the operation goes on at once. So does the reconnect's delivery of what it read during its
     * replay: the subscription handlers can then get messages from the new connection before the listener hears
     * of it.
     *
     * Either way the listener hears of the connection only while it is still the open one: not once it is closed
     * or being closed, nor once it was replaced, and not once it was lost - then the listener hears of neither
     * the open nor the loss ({@see performRecovery()}). A Reconnected listener call therefore always finds the
     * connection Open, and never comes after the Closed that ended it.
     */
    private function announceOpen(ConnectionEvent $event): void
    {
        $generation = $this->connectionGeneration;
        $this->unannouncedOpen = ['event' => $event, 'generation' => $generation];
        $this->logLifecycleEvent($event, null);

        if ($this->openListenerCalls > 0) {
            EventLoop::queue(function () use ($generation): void {
                $this->notifyOpen($generation);
            });

            return;
        }

        $this->notifyOpen($generation);
    }

    /**
     * Calls the connection listener for the open of the connection of $generation, if it is still the open one
     * and the listener has not heard of it yet ({@see announceOpen()}).
     */
    private function notifyOpen(int $generation): void
    {
        $unannounced = $this->unannouncedOpen;
        // Lost first: performRecovery() dropped the announcement.
        if ($unannounced === null || $unannounced['generation'] !== $generation) {
            return;
        }

        $this->unannouncedOpen = null;
        // Closed or being closed, or replaced since by a connect() after a close. A loss would have dropped the
        // announcement (see above), so the state is Open unless the user closed the connection; it is checked all
        // the same, as the one thing a Reconnected listener may count on.
        if ($this->closing || $this->connectionGeneration !== $generation || $this->state !== ConnectionState::Open) {
            return;
        }

        $this->notifyConnectionListener($unannounced['event'], null);
    }

    /**
     * Logs an asynchronous error and invokes the configured error listener, swallowing any exception
     * either raises ({@see emitErrorSafely()}).
     *
     * @internal Public only so client-side buffers (SubscriptionQueue slow-consumer drops, #134)
     *           can report through the same listener/logger; not part of the supported API.
     */
    public function emitError(\Throwable $error, string $logLevel = 'error'): void
    {
        // Routine, high-frequency conditions (slow-consumer drops) log at debug so they cannot flood
        // error logs on a per-message hot path; genuine errors stay at error level. The error listener
        // is always notified regardless of level (callers opted in and can throttle themselves), also
        // when the logger throws: a SubscriptionQueue's drop report used to fail the read that delivered
        // it, and under DropOldest lose the message it had just made room for.
        $this->emitErrorSafely($error, $logLevel);
    }

    private function logError(\Throwable $error, string $logLevel): void
    {
        $this->logger->log($logLevel, 'NATS connection error: ' . $error->getMessage(), ['exception' => $error]);
    }

    private function notifyErrorListener(\Throwable $error): void
    {
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
     *
     * The lame-duck failover (#47) runs inline, in the fiber that dispatches the INFO, for your own read
     * (processIncoming(), readIncoming()), a serving loop's, the heartbeat's, the dispatches of a connect or a
     * reconnect, where it is a no-op ({@see recoverConnection()}), and any read's dispatch of the frames it parsed
     * ahead of bytes that fail to parse, whose corrupt stream it recovers from inline right after: this returns
     * with the connection on another server, or ended when no server could be reached (the failure is reported),
     * or closed when the LameDuck listener closed it itself, which leaves no failover to run. The dispatch of a
     * chunk that a read with a wake-up read and parsed, the read of one of the library's operations or a flush's
     * for its PONG ($failOverInItsOwnFiber), starts it in a fiber of its own instead and returns at once
     * ({@see startLameDuckFailover()}, #191): the read waits for it once it has delivered its chunk, and only
     * within the operation's own wait ({@see awaitLameDuckFailoverFrom()}), so that the dials, the handshake, the
     * replay and any backoff no longer keep the operation past its deadline. Not under close-intent: the failover
     * would not run then, and the rest of the chunk stays the connection's own, so that a drain()'s flush still
     * answers a PING behind the INFO on the connection it is closing. Either way the failover is for the connection
     * the INFO came on: one that a reconnect opened while a listener of the INFO was suspended is left alone. The
     * INFO's chunk can hold more frames behind it, the old server's, and the dispatch handles them for the
     * connection they were read on: their messages are queued, and their PING, PONG, INFO and -ERR are not applied
     * to the new connection ({@see dispatchFrames()}, {@see handleStaleFrame()}, #182), from the moment the
     * failover is started when it runs in a fiber of its own.
     *
     * @param bool $failOverInItsOwnFiber Start the failover in a fiber of its own rather than run it inline: for the
     *        dispatch of a chunk that a read with a wake-up read and parsed ({@see readChunk()}).
     */
    private function handleServerInfoUpdate(bool $failOverInItsOwnFiber = false): void
    {
        $info = $this->serverInfo;
        if ($info === null) {
            return;
        }

        // The connection the INFO came on. The listeners below can suspend, and a reconnect replace that connection
        // meanwhile: the failover is then not for the connection the reconnect opened, which it would fail over again.
        $generation = $this->connectionGeneration;

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
                // The read of one of the library's operations does not run it: it waits for it within the operation's
                // own wait (#191). Not under close-intent, where the failover would not run: no mark is left for it.
                if ($failOverInItsOwnFiber && !$this->closing) {
                    $this->startLameDuckFailover($generation);

                    return;
                }

                try {
                    $this->recoverConnection(failedGeneration: $generation);
                } catch (\Throwable $e) {
                    $this->emitError($e);
                }
            }
        }
    }

    /**
     * Starts the lame-duck failover in a fiber of its own (#191), for a read with a wake-up whose chunk brought the
     * INFO ({@see handleServerInfoUpdate()}), and marks the connection it leaves as being left
     * ({@see $lameDuckFailover}). The failover is the reconnect the inline path runs: no cause, since a lame duck is
     * not a failure, and its failure reported through the error listener, as the inline path reports it, so that
     * the fiber's future never errors. A connection replaced since the INFO came, while a listener of the INFO's
     * dispatch was suspended, is not failed over ($failedGeneration). The mark is cleared when the failover returns,
     * unless a later failover has replaced it.
     *
     * The fiber runs only once the dispatching fiber suspends: until then the state is still Open and the generation
     * unchanged, and the mark is what tells {@see dispatchFrames()} that the rest of the chunk is the leaving
     * server's. It is the recovery fiber, as for a reconnect an operation's failed read starts
     * ({@see recoverAfterFailedOperationRead()}): a listener called during the failover runs inside it, and an
     * operation issued from there is refused a join ({@see awaitOpenConnection()}). The new connection is announced
     * from it as well, once the failover is over, and the read that waits for it waits for that listener too while
     * its own wait lasts.
     *
     * @param int $generation The {@see $connectionGeneration} of the connection the INFO came on.
     */
    private function startLameDuckFailover(int $generation): void
    {
        $failover = async(function () use ($generation): void {
            try {
                $this->recoverConnection(failedGeneration: $generation);
            } catch (\Throwable $e) {
                $this->emitError($e);
            } finally {
                if ($this->lameDuckFailover !== null && $this->lameDuckFailover['generation'] === $generation) {
                    $this->lameDuckFailover = null;
                }
            }
        });
        $this->lameDuckFailover = ['generation' => $generation, 'failover' => $failover];
    }

    /**
     * Waits for a lame-duck failover started in a fiber of its own ({@see startLameDuckFailover()}, #191) away from
     * the connection of $generation, for a read with a wake-up that read on that connection ({@see readChunk()}),
     * once it has delivered its chunk: whether its own dispatch started the failover or another fiber's read of the
     * same connection did, that connection is being left. Returns at once when no such failover is under way.
     *
     * Only within the operation's own wait, as the read waits for a reconnect its failed read starts
     * ({@see recoverAfterFailedOperationRead()}, #178): the deadline ends the wait with the caller's cancellation,
     * so that the operation times out while the failover carries on; the wake-up ends it with the read returning,
     * so that the operation looks again and finds what was delivered meanwhile; and a failover that ends, however it
     * ends, ends it as well, the operation then looking again before it reads the new connection, or finding the
     * connection closed. With waiting for a reconnect disabled the read fails at once with "Connection is not open",
     * as every operation does while a reconnect is in flight, unless its wait has already ended: the wake-up still
     * wins, so a reply that came in the chunk is returned. Nor once the failover has replaced the connection, which a
     * handler of the chunk that awaited lets it do before the read gets here: the read returns, and the operation
     * looks again, on the new connection when the failover is only announcing it. Failing here rather than at the
     * operation's next read keeps the outcome from depending on how far the failover has got by then: a flush left
     * to its next read would find either its PONG failed by the failover's first attempt or the connection not
     * open, depending on how long the transport's close took, as #178 found for a read that meets a lost
     * connection. A flush whose PONG came in the chunk does not get here ({@see readChunk()}).
     *
     * @param int $generation The {@see $connectionGeneration} the read read on.
     * @param Cancellation|null $cancellation The caller's cancellation: the operation's deadline.
     * @param Cancellation $wake The read's wake-up.
     *
     * @throws CancelledException When the deadline ends the wait: the caller's own exception.
     * @throws ConnectionException At once when waiting for a reconnect is disabled, unless the wait has already ended
     *         or the connection has been replaced.
     */
    private function awaitLameDuckFailoverFrom(int $generation, ?Cancellation $cancellation, Cancellation $wake): void
    {
        $started = $this->lameDuckFailover;
        if ($started === null || $started['generation'] !== $generation) {
            return;
        }

        $waitCancellation = self::waitWith($cancellation, $wake);
        try {
            if (!$this->options->waitForReconnect) {
                // Not when the operation's wait has already ended: what it waits for came in the chunk, or its deadline
                // passed, and the catch below tells which.
                $waitCancellation?->throwIfRequested();

                // Nor once the failover has replaced the connection this read read on, as a handler of this read's
                // chunk that awaited lets it: the operation looks again, and runs on the new connection when the
                // failover is only announcing it, or fails at once as any operation does while the failover is still
                // under way.
                if ($this->connectionGeneration === $generation) {
                    throw new ConnectionException('Connection is not open');
                }

                return;
            }

            $started['failover']->await($waitCancellation);
        } catch (CancelledException $cancelled) {
            if (!self::wokenUp($wake, $cancellation)) {
                // The caller's own exception when both fired: the wake-up's may have won inside the composite.
                $cancellation?->throwIfRequested();

                throw $cancelled;
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
     *
     * @param list<SlowConsumerException>|null $heldOverflows When an array, an overflow of this
     *        subscription's own SubscriptionQueue buffer - thrown by its handler once the queue dropped and
     *        counted the message - does not end the delivery: with $reportOverflows it is reported at once
     *        (unless $sid is $ownSid), otherwise it is held here, to be thrown. When null such an overflow
     *        ends the delivery.
     * @param list<\Throwable>|null $heldFailures When an array, anything else the handler throws that is not
     *        reported (see $reportHandlerFailures) does not end the delivery either (#177): it is held here, to
     *        be thrown once the pass is over, and this subscription's delivery stops at the failing message for
     *        this pass, the rest of it left queued, in order, and the sid dirty and marked as stopped
     *        ({@see $stoppedByAFailure}), so that the next pass continues it: your own next read makes one before
     *        it reads (#186). The other subscriptions' messages queued behind it are then still delivered. When
     *        null such a failure ends the delivery and propagates, and the sid is not marked: that caller,
     *        drainSubscription(), reports it and delivers the rest itself.
     * @param bool $reportHandlerFailures Report anything else the handler throws at once and go on, unless
     *        $sid is $ownSid: the handler of an operation's own subscription that fails still fails the
     *        operation.
     * @param-out ($heldOverflows is null ? null : list<SlowConsumerException>) $heldOverflows
     * @param-out ($heldFailures is null ? null : list<\Throwable>) $heldFailures
     * @return int How many messages this pass handed to the handler, a message its handler threw on included:
     *         zero when the sid had nothing to deliver, is already being delivered further up the stack, or was
     *         dropped at its auto-unsubscribe cap before a delivery ({@see deliverQueuedForOwnSid()}).
     */
    private function drainPendingForSid(
        int $sid,
        ?array &$heldOverflows = null,
        ?array &$heldFailures = null,
        bool $reportOverflows = false,
        ?int $ownSid = null,
        bool $reportHandlerFailures = false,
    ): int {
        $queue = $this->pendingMessages[$sid] ?? null;
        if ($queue === null) {
            return 0;
        }

        if (!isset($this->subscriptions[$sid])) {
            // The subscription is gone; its backlog is undeliverable. Drop it (and its dirty-set entry and
            // stopped mark) instead of retaining state that drainAllPending() would re-scan.
            unset($this->pendingMessages[$sid], $this->pendingDirty[$sid], $this->stoppedByAFailure[$sid]);

            return 0;
        }

        if ($queue->isEmpty()) {
            // Nothing buffered. The queue persists empty until the subscription is dropped (#139) - the
            // previous unset-on-empty meant one SplQueue alloc/free per delivered message in the
            // promptly-drained common case. Clear any dirty-set entry so the invariant (dirty IFF
            // non-empty) holds even when a reentrant drain emptied this sid earlier in the same pass, and
            // the stopped mark with it: nothing is left to continue (#186).
            unset($this->pendingDirty[$sid], $this->stoppedByAFailure[$sid]);

            return 0;
        }

        if (isset($this->dispatchingSids[$sid])) {
            // Already delivering this sid further up the stack (a handler awaited and suspended). Do
            // not re-enter: the suspended loop resumes and drains whatever we enqueued meanwhile, so
            // ordering holds and a handler is never invoked on top of itself.
            return 0;
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
                    && $this->monotonicSeconds() >= $this->drainDeadline
                ) {
                    // The drain budget ran out mid-pass: stop before the NEXT delivery, so a handler
                    // that consumes budget per message (e.g. a bounded ack publish timing out per
                    // delivery) cannot serially extend drain() a whole pass (~K x the per-publish
                    // bound) past its deadline. The remainder stays queued (the sid stays dirty via
                    // the finally below) and drain()'s own deadline check then reports the discard
                    // loudly, exactly as it does for a backlog that never got a pass (#149). Keyed on
                    // the deadline alone, not the Draining state: a drain that winds down without a
                    // connection (the reconnect outlasted its budget) is bounded the same way.
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
                try {
                    $this->subscriptions[$sid]($message);
                } catch (SlowConsumerException $overflow) {
                    if ($heldOverflows !== null && $overflow->sid === $sid) {
                        // This subscription's SubscriptionQueue buffer is full: the queue dropped the message
                        // and counted it. The subscriber fell behind - no reason to stop delivering. The
                        // message counts toward this pass like any other, and a report is made here, at
                        // once, so drain()'s deadline check above also counts the time a slow listener takes.
                        // (An overflow of another sid is the handler's own failure: one its own read threw.)
                        if ($reportOverflows && $sid !== $ownSid) {
                            $this->emitErrorSafely($overflow);
                        } else {
                            $heldOverflows[] = $overflow;
                        }
                    } elseif ($reportHandlerFailures && $sid !== $ownSid) {
                        $this->emitErrorSafely($overflow);
                    } elseif ($heldFailures !== null) {
                        // Held like any other failure of the handler, below: this subscription's delivery
                        // stops here for this pass, marked for your next read, and the other subscriptions'
                        // messages are still delivered.
                        $heldFailures[] = $overflow;
                        $this->stoppedByAFailure[$sid] = true;

                        break;
                    } else {
                        throw $overflow;
                    }
                } catch (\Throwable $handlerFailure) {
                    if ($reportHandlerFailures && $sid !== $ownSid) {
                        $this->emitErrorSafely($handlerFailure);
                    } elseif ($heldFailures !== null) {
                        // The failure is the read's to throw, once the pass has delivered the other
                        // subscriptions' messages (#177). This subscription's delivery stops at the failing
                        // message: the rest of it stays queued, in order, and the finally below keeps the sid
                        // dirty, so the next pass continues it, and throws again if the next handler throws.
                        // Marked, so that your own next read makes that pass before it reads (#186), rather
                        // than once a read receives anything; the finally below clears the mark when nothing
                        // is left.
                        $heldFailures[] = $handlerFailure;
                        $this->stoppedByAFailure[$sid] = true;

                        break;
                    } else {
                        throw $handlerFailure;
                    }
                }

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

            // An operation reading for this subscription looks again ({@see nextDeliveryTo()}): its result may be in,
            // whichever read made this delivery, the operation's own, another fiber's or a reconnect's. Once per pass:
            // on an operation's own subscription the handler is the library's, and a pass there suspends only in an
            // error listener that a dropped message is reported to, when this fires once the pass is over.
            if ($deliveredThisPass > 0) {
                $this->wakeReadsWaitingFor($sid);
            }

            // drainSubscription() left this subscription's removal to this delivery: remove it once
            // everything queued has been handed over. A delivery cut short - a handler that threw, whose
            // failure the pass holds or throws, or drain()'s budget - leaves the rest to the next pass, or to
            // drain()'s report and teardown.
            if (isset($this->removeAfterDelivery[$sid]) && (!isset($this->pendingMessages[$sid]) || $this->pendingMessages[$sid]->isEmpty())) {
                $this->dropSubscriptionState($sid);
            }

            // Run in finally so a handler that throws mid-drain still triggers the terminal cleanup
            // rather than stranding the subscription (#112). completeAutoUnsub handles the
            // slow-consumer case where delivered stays below max (dropped messages) but the
            // server-side max was still reached: it drops on received>=max once the backlog is
            // drained. The drained queue itself is deliberately kept (empty) for reuse - see the
            // early return above (#139); dropSubscriptionState() removes it with the subscription.
            $this->completeAutoUnsubIfSatisfied($sid);

            // Keep the dirty set = sids with a non-empty queue: once this sid's queue has fully drained
            // (or the subscription was dropped by an auto-unsub cap / completeAutoUnsub above), remove it
            // so drainAllPending() no longer scans it, and the stopped mark with it (#186). A handler that
            // threw mid-drain leaves this subscription's later messages queued with the subscription still
            // alive - only its own: the pass goes on to the other sids (#177) - and the sid stays dirty,
            // re-drained next pass (#162), and marked as stopped until nothing is left.
            if (!isset($this->pendingMessages[$sid]) || $this->pendingMessages[$sid]->isEmpty()) {
                unset($this->pendingDirty[$sid], $this->stoppedByAFailure[$sid]);
            }
        }

        return $deliveredThisPass;
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
        // Nor the guard of a guarded inbox (#175): nothing is left to confirm or reject once the sid is gone. The
        // pong slot of the PING behind its SUB stays queued, since that PING's PONG is still owed.
        unset($this->guardedSids[$sid]);
        unset($this->unconfirmedSids[$sid]);
        unset($this->removeAfterDelivery[$sid]);
        // Nor a stopped mark (#186): its queue went with it, and your next read has nothing to continue.
        unset($this->stoppedByAFailure[$sid]);
        // Nothing more will be delivered to it: an operation's read still waiting for a delivery looks again.
        $this->wakeReadsWaitingFor($sid);
    }
}
