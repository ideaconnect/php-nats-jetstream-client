<?php

declare(strict_types=1);

namespace IDCT\NATS\Core;

use Amp\Cancellation;
use Amp\Future;
use IDCT\NATS\Connection\ConnectionStats;
use IDCT\NATS\Connection\DrainParticipant;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\IncomingChunkResult;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Exception\SlowConsumerException;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\Protocol\ServerInfo;
use IDCT\NATS\Services\Service;
use IDCT\NATS\Transport\AmpSocketTransport;
use IDCT\NATS\Transport\TransportInterface;

use function Amp\async;

/**
 * Facade client exposing high-level NATS publish/subscribe and request APIs.
 */
final class NatsClient
{
    private readonly NatsConnection $connection;
    private readonly NatsOptions $options;
    private ?JetStreamContext $jetStreamContext = null;

    /**
     * Creates a high-level client facade over the connection runtime.
     *
     * @param NatsOptions $options Runtime options for NATS connectivity, authentication, heartbeat, and reconnect behavior.
     * @param TransportInterface|null $transport Optional custom transport. When null, AmpSocketTransport is used.
     */
    public function __construct(
        NatsOptions $options = new NatsOptions(),
        ?TransportInterface $transport = null,
    ) {
        $this->options = $options;
        $this->connection = new NatsConnection(
            options: $options,
            transport: $transport ?? new AmpSocketTransport($options),
        );
    }

    /**
     * Opens a connection to the configured NATS server.
     *
     * @return Future<void>
     */
    public function connect(): Future
    {
        return $this->connection->connect();
    }

    /**
     * Closes the active connection and releases underlying transport resources.
     *
     * Locally queued, undelivered messages are discarded without being delivered - nats.go
     * Close() parity (#134). Use {@see drain()} for the lossless path: it delivers the buffered
     * backlog before closing. What a JetStream pull consumer run's pulls hold is discarded the same
     * way, wherever the run is (#207): a run this ends hands its handler none of it, and a run handing
     * a pull over, in its handler for one of the pull's messages say, stops before the next message,
     * the rest left unacked for the server to deliver again after the ack wait (lost on a consumer
     * without acks). The run's handle() then fails, with "Connection is not open" or the error of the
     * read or write that met the close, except where the client's drain() had already asked the run
     * for its hand-over, or where a finite run has no pull left to issue: it then resolves with its
     * count.
     *
     * @return Future<void>
     */
    public function disconnect(): Future
    {
        return $this->connection->disconnect();
    }

    /**
     * Gracefully drains all subscriptions, flushes pending messages, and closes.
     *
     * While a reconnect is in flight it first waits for it within its budget
     * ({@see NatsOptions::$waitForReconnect}), so the publishes buffered during the outage are flushed;
     * if the budget runs out first it still closes. When it cannot wait it closes the connection and
     * throws. Every drain ends with one {@see \IDCT\NATS\Connection\Enum\ConnectionEvent::Closed}
     * event, and connect() is refused until it is over.
     *
     * A JetStream pull consumer run's pulls are drained too (#207): once the drain's flush is done, each
     * run hands its handler what its pulls hold, in order, while the connection is Draining, so that the
     * acks the handler publishes (ack(), nak(), term(), inProgress()) still go out, and the drain waits
     * for that, the handler included, within the same budget. The handler's requests work too (#213): the
     * drain keeps the shared reply inbox subscribed while it delivers, sets it up for the client's first
     * request, and releases it once its delivery phase is over, so an ackSync() is confirmed and a
     * JetStream publish or a Key/Value read or write gets its reply. A request still waiting when that phase
     * ends - everything delivered, or the budget run out - fails with "Connection is not open"; another
     * fiber's request is taken as well, but does not extend the phase. Subscriptions, and the operations that
     * need one of their own (fetchBatch(), batched Direct Get, Key/Value keys() and history()), stay refused.
     * From the deadline on no handler gets another message. The run then ends, and its handle() resolves
     * with its count, as after the iterator's
     * {@see \IDCT\NATS\JetStream\Consumers\PullConsumerIterator::drain()}. When the budget runs out
     * first, the drain closes the connection, and what the run still holds is discarded from the "drain
     * deadline exceeded" report on, which counts it, as a disconnect() discards it. A drain without a
     * connection to drain (its budget ran out before the reconnect it waited for was done, or that
     * reconnect gave up) asks no run: the run's read fails, and what it holds is discarded. Do not await
     * it from code the run calls (its handler, its onError, the error listener while the run reports to
     * it): the drain waits for the very run that waits for it, to the end of its budget, as it does from
     * a subscription's handler that awaits it. Stop or drain the iterator there and drain the client
     * once handle() has resolved, or call this from another fiber, or without awaiting it.
     *
     * @return Future<void>
     */
    public function drain(): Future
    {
        return $this->connection->drain();
    }

    /**
     * Publishes a payload to a subject.
     *
     * @return Future<void>
     */
    public function publish(string $subject, string $payload, ?string $replyTo = null): Future
    {
        return $this->connection->publish($subject, $payload, $replyTo);
    }

    /**
     * @internal Budgeted batch publication; callbacks must not suspend.
     * @param \Closure():?string $payload
     * @param \Closure():void $beforeWrite
     * @return Future<void>
     */
    public function publishWithin(string $subject, \Closure $payload, string $replyTo, int $replySid, Cancellation $budget, \Closure $beforeWrite): Future
    {
        return $this->connection->publishWithin($subject, $payload, $replyTo, $replySid, $budget, $beforeWrite);
    }

    /**
     * @internal Releases a temporary inbox without allowing its cleanup to overrun the operation.
     * @return Future<void>
     */
    public function releaseSubscriptionWithin(int $sid, Cancellation $budget): Future
    {
        return $this->connection->releaseSubscriptionWithin($sid, $budget);
    }

    /**
     * Publishes a payload with NATS headers to a subject. A header value may be a single string or a
     * list of strings for multi-value (multimap) headers (ADR-4).
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
        return $this->connection->publishWithHeaders($subject, $payload, $headers, $replyTo);
    }

    /**
     * Publishes a block of header-carrying messages coalesced into bounded segment writes,
     * preserving frame order and per-message validation (#138).
     *
     * @internal Used by BatchPublisher to send atomic-batch intermediates with one wire write per
     *           segment instead of one awaited write per message; not part of the supported API.
     *
     * @param list<array{subject:string,payload:string,headers:array<string,string|list<string>>}> $messages
     * @return Future<void>
     */
    public function publishHeaderBlock(array $messages): Future
    {
        return $this->connection->publishHeaderBlock($messages);
    }

    /**
     * Registers a subscription handler and returns its SID.
     *
     * While a reconnect is in flight it first waits for it within the request timeout
     * ({@see NatsOptions::$waitForReconnect}).
     *
     * @param callable(NatsMessage):void $handler
     * @return Future<int>
     */
    public function subscribe(string $subject, callable $handler, ?string $queue = null): Future
    {
        return $this->connection->subscribe($subject, $handler, $queue);
    }

    /**
     * Subscribes a reply inbox whose rejection by the connection's subscription limit must reach the operation
     * waiting on it, whichever fiber's read meets the -ERR (#175): the JetStream pull fetch, pull pipeline and
     * batched Direct Get inboxes. A PING is written behind the SUB and the inbox counts as held by the server once
     * that PING's PONG, or a delivery on the inbox, arrives; a 'maximum subscriptions exceeded' -ERR read before
     * that drops the subscription, writes its UNSUB and calls $onRejected with the -ERR text, as does a permissions
     * violation naming the subject (which leaves the subscription for the caller to unsubscribe). Several inboxes
     * unconfirmed at once are all treated as rejected by such an -ERR. A SUB whose write failed is replayed by the
     * reconnect; when the new server rejects the replay, the subscribe itself fails with a ConnectionException
     * naming the subject and the limit, $onRejected called first. See {@see NatsConnection::subscribeGuarded()}.
     * An optional operation budget covers reconnect and SUB write backpressure instead of the global timeout.
     *
     * @internal Low-level mechanism for the JetStream reply inboxes; not part of the supported API.
     *
     * @param callable(NatsMessage):void $handler
     * @param \Closure(string): void $onRejected Must not suspend: it runs inside the dispatch of the -ERR.
     * @return Future<int>
     */
    public function subscribeGuarded(string $subject, callable $handler, \Closure $onRejected, ?Cancellation $budget = null): Future
    {
        return $this->connection->subscribeGuarded($subject, $handler, $onRejected, $budget);
    }

    /**
     * Exempts a subscription from the slow-consumer pending-queue bound (its replies are never
     * dropped). For the muxed request inbox (#118) and the pipelined pull inbox (#120), where a
     * dropped reply silently breaks a request/pull. Synchronous so it applies in the same tick the
     * sid was returned by {@see subscribe()}. A sid no longer registered is left alone.
     *
     * @internal Low-level mechanism for the JetStream request/pull inboxes; not general API.
     */
    public function markSubscriptionUnbounded(int $sid): void
    {
        $this->connection->markSubscriptionUnbounded($sid);
    }

    /**
     * Registers a callback invoked when the server rejects the sid's subscription with an async
     * permissions -ERR, so a long-lived JetStream reply inbox (the pipelined pull inbox, #120) can
     * fail fast instead of spinning on silent deadline retires forever (#167's pull twin).
     *
     * @internal Low-level mechanism for the JetStream reply inboxes; not part of the supported API.
     */
    public function onSubscriptionRejected(int $sid, \Closure $handler): void
    {
        $this->connection->markSubscriptionRejectionHandler($sid, $handler);
    }

    /**
     * Whether a server -ERR text is the one rejecting a SUB beyond the connection's subscription limit
     * ('maximum subscriptions exceeded'), which names no subject, as the connection classifies it
     * ({@see NatsConnection::isSubscriptionLimitError()}): for the rejection handler of {@see subscribeGuarded()},
     * which is also called with a permissions violation naming the subject.
     *
     * @internal Low-level mechanism for the JetStream reply inboxes; not part of the supported API.
     */
    public function isSubscriptionLimitError(string $serverError): bool
    {
        return $this->connection->isSubscriptionLimitError($serverError);
    }

    /**
     * Subscribes and returns a SubscriptionQueue for polling-style message consumption.
     *
     * @return Future<SubscriptionQueue>
     */
    public function subscribeQueue(string $subject, ?string $queue = null): Future
    {
        return async(function () use ($subject, $queue): SubscriptionQueue {
            /** @var SubscriptionQueue|null $subscriptionQueue */
            $subscriptionQueue = null;
            /** @var \SplQueue<NatsMessage> $early */
            $early = new \SplQueue();
            $sid = $this->connection->subscribe(
                $subject,
                static function (NatsMessage $msg) use (&$subscriptionQueue, $early): void {
                    // The sid is routable the moment SUB hits the wire - before the queue object
                    // exists. A delivery in that window (a concurrent read or the heartbeat
                    // self-read) must be buffered, not silently dropped (#129).
                    if ($subscriptionQueue === null) {
                        $early->enqueue($msg);

                        return;
                    }

                    $subscriptionQueue->enqueue($msg);
                },
                $queue,
            )->await();
            $subscriptionQueue = new SubscriptionQueue(
                $this,
                $sid,
                $this->options->maxPendingMessagesPerSubscription,
                $this->options->slowConsumerPolicy,
            );

            // Replay through enqueue() so the cap and slow-consumer policy apply as usual. A message that does
            // not fit (SlowConsumerPolicy::Error) is counted and reported: the caller does not have the queue
            // yet, so there is nobody to throw it to, and failing here would leave the subscription feeding a
            // queue nobody holds.
            while (!$early->isEmpty()) {
                try {
                    $subscriptionQueue->enqueue($early->dequeue());
                } catch (SlowConsumerException $overflow) {
                    $this->connection->emitError($overflow);
                }
            }

            return $subscriptionQueue;
        });
    }

    /**
     * Reports whether a subscription with the given sid is still registered (dropped by
     * unsubscribe/drain or a terminal close). Lets the JetStream idle-heartbeat watchdog self-cancel
     * when the subscription it guards is torn down (#113).
     *
     * @internal Not part of the supported public API.
     */
    public function isSubscriptionActive(int $sid): bool
    {
        return $this->connection->isSubscriptionActive($sid);
    }

    /**
     * Whether the application has closed the connection, or is closing it: disconnect() or drain() was called since the
     * last connect() ({@see NatsConnection::isCloseRequested()}). The pull consumer engine asks it whether an infinite
     * run goes on past a frame that ended the connection (#210); whether what a run holds is discarded is
     * {@see isDiscardingUndelivered()}'s to say (#207).
     *
     * @internal For the pull consumer engine (#197, #210); not part of the supported API.
     */
    public function isCloseRequested(): bool
    {
        return $this->connection->isCloseRequested();
    }

    /**
     * Whether what the connection has received and not handed to a handler is a close's to discard: a disconnect()
     * under way or done, or a drain() whose budget has run out (from its deadline report on) or that has none to
     * drain, but not a drain() within its budget, which delivers it
     * ({@see NatsConnection::isDiscardingUndelivered()}). The pull consumer engine asks it before each message it
     * hands to the handler, wherever the run is (#207).
     *
     * @internal For the pull consumer engine (#207); not part of the supported API.
     */
    public function isDiscardingUndelivered(): bool
    {
        return $this->connection->isDiscardingUndelivered();
    }

    /**
     * Registers what a drain() of this client waits for besides the connection's own backlog, and returns the id
     * {@see removeDrainParticipant()} takes ({@see NatsConnection::addDrainParticipant()}): a pull consumer run, whose
     * pulls a drain() asks it to hand over once its flush is done (#207).
     *
     * @internal For the pull consumer engine (#207); not part of the supported API.
     */
    public function addDrainParticipant(DrainParticipant $participant): int
    {
        return $this->connection->addDrainParticipant($participant);
    }

    /**
     * Removes a participant {@see addDrainParticipant()} registered ({@see NatsConnection::removeDrainParticipant()}).
     *
     * @internal For the pull consumer engine (#207); not part of the supported API.
     */
    public function removeDrainParticipant(int $id): void
    {
        $this->connection->removeDrainParticipant($id);
    }

    /**
     * Whether a failure that a read of this client threw came from a frame that ended the connection: a fatal -ERR,
     * or a PONG the socket would not take, as the connection classifies it ({@see NatsConnection::endedTheConnection()}).
     * The read recovered the connection before it threw (#171), so the failure says why the old connection ended, not
     * that the connection is gone.
     *
     * @internal For the pull consumer engine (#210); not part of the supported API.
     */
    public function endedTheConnection(\Throwable $failure): bool
    {
        return $this->connection->endedTheConnection($failure);
    }

    /**
     * Removes a subscription by SID.
     *
     * With $maxMessages, arms auto-unsubscribe: delivery continues until that many TOTAL messages
     * (counting already-delivered ones) have reached the handler, then the subscription is removed
     * automatically - matching the server-side `UNSUB <sid> <max>` semantics (#112). On a connection
     * that is not open, local state is released silently instead of throwing (#116).
     *
     * A plain unsubscribe (no $maxMessages) discards the sid's locally queued, undelivered backlog -
     * nats.go Unsubscribe() parity (#134). Use {@see drainSubscription()} (or a full {@see drain()})
     * for the lossless path that delivers the backlog first.
     *
     * @return Future<void>
     */
    public function unsubscribe(int $sid, ?int $maxMessages = null): Future
    {
        return $this->connection->unsubscribe($sid, $maxMessages);
    }

    /**
     * Drains a single subscription: stops new deliveries (UNSUB), flushes so in-flight messages are
     * dispatched to the handler, then removes the subscription. Mirrors per-subscription Drain().
     *
     * One budget (~requestTimeoutMs) covers it all. While reconnecting it waits for the reconnect, like
     * flush(), and then drains on the new connection; a reconnect never re-subscribes a subscription being
     * drained. During a drain() the drain takes the subscription over. Failures, and messages it cannot
     * deliver, are reported through the error listener.
     *
     * @return Future<void>
     */
    public function drainSubscription(int $sid): Future
    {
        return $this->connection->drainSubscription($sid);
    }

    /**
     * Processes a single incoming transport chunk and dispatches parsed frames. While a reconnect is in
     * flight it first waits for it, bounded by the cancellation ({@see NatsOptions::$waitForReconnect}).
     * When another fiber is reading the socket (a request waiting for its reply, the heartbeat), it waits
     * for that read to finish, bounded by the cancellation, and returns 0: that read delivered what it read.
     * Before it reads, or waits for another fiber's read, it continues the subscriptions whose delivery an
     * earlier read stopped at a handler that threw, and throws the next such failure without reading (#186);
     * the count is still of the frames it read. The cancellation bounds the read's waits, not that delivery: a
     * read whose cancellation has already fired still makes it, then throws ({@see NatsConnection::readIncoming()}).
     *
     * @param Cancellation|null $cancellation Optional token that cancels the underlying socket read.
     * @return Future<int>
     */
    public function processIncoming(?Cancellation $cancellation = null): Future
    {
        return $this->connection->processIncoming($cancellation);
    }

    /**
     * Processes a single incoming transport chunk and dispatches parsed frames, reporting both the
     * frame count and whether the read consumed bytes off the wire. A wait loop uses the latter to
     * skip its 1 ms idle sleep on partial-frame progress (a large payload arriving in socket-sized
     * chunks) and yield only on a genuinely idle read (#119). {@see processIncoming()} is the
     * frame-count-only view of the same cycle, and waits for another fiber's read, and continues the
     * subscriptions an earlier read stopped at a handler that threw, the same way.
     *
     * @param Cancellation|null $cancellation Optional token that cancels the underlying socket read.
     * @return Future<IncomingChunkResult>
     */
    public function readIncoming(?Cancellation $cancellation = null): Future
    {
        return $this->connection->readIncoming($cancellation);
    }

    /**
     * {@see readIncoming()} for an operation that reads while it waits for a result of its own: under
     * SlowConsumerPolicy::Error, another subscription's overflow is reported instead of failing it
     * ({@see NatsOptions::$slowConsumerErrorsFailOperations}), and so is a handler of another subscription
     * that throws ({@see NatsOptions::$handlerErrorsFailOperations}).
     *
     * @internal For the library's own operations (JetStream, Key/Value, polling queues, services); not
     *           part of the supported API.
     *
     * @param int|null $ownSid The operation's own subscription, whose overflow or failing handler still fails
     *        the operation, and whose next delivery, by whichever fiber's read, ends the read without reading
     *        ({@see NatsConnection::readIncomingForOperation()}): call this right after looking for the result.
     *        What an earlier read has already queued for it, behind a delivery held up in a lower sid's handler
     *        that awaits, the handler the operation itself runs in included, is delivered first, and the read
     *        returns without reading (#179); the other subscriptions' queued messages stay in order for that
     *        delivery.
     * @param bool $alwaysReport Report every overflow and every handler that throws while the read delivers,
     *        whatever the options say, the messages behind it still delivered, and an -ERR the server keeps the
     *        connection open for: for a read whose caller would only swallow them, such as a serving loop. Such
     *        a read also delivers what an earlier or a concurrent read has queued and not reached - the later sids
     *        of a delivery held up in another fiber's handler, the rest of a subscription whose handler threw in
     *        the application's read - unless a disconnect() is closing the connection, which discards it. A read
     *        that receives anything during the close still delivers it with what it received, as any read does. A
     *        failure that ends the connection is still thrown, once the connection has recovered.
     * @return Future<IncomingChunkResult>
     */
    public function readIncomingForOperation(?Cancellation $cancellation = null, ?int $ownSid = null, bool $alwaysReport = false): Future
    {
        return $this->connection->readIncomingForOperation($cancellation, $ownSid, $alwaysReport);
    }

    /**
     * Routes an asynchronous error through the connection's error listener and logger.
     *
     * @internal Used by SubscriptionQueue to surface slow-consumer drops (#134); not part of the
     *           supported API.
     */
    public function emitError(\Throwable $error, string $logLevel = 'error'): void
    {
        $this->connection->emitError($error, $logLevel);
    }

    /**
     * Flushes outbound writes and waits for the server's PONG, confirming the server has processed
     * everything sent so far (e.g. a SUBSCRIBE before a dependent request). Bounded by the request
     * timeout, one budget for the whole flush. While a reconnect is in flight it first waits for it
     * within that budget ({@see NatsOptions::$waitForReconnect}).
     *
     * @return Future<void>
     */
    public function flush(): Future
    {
        return $this->connection->flush();
    }

    /**
     * {@see flush()} that reports what its reads meet - a full subscription queue, a handler that throws, an
     * -ERR the server keeps the connection open for - through the error listener and reads on to its PONG,
     * instead of failing with it.
     *
     * @internal For the service framework's drain(); not part of the supported API.
     *
     * @return Future<void>
     */
    public function flushReportingFailures(): Future
    {
        return $this->connection->flushReportingFailures();
    }

    /**
     * Returns the current connection state (Open, Connecting, Draining, Closed, ...).
     */
    public function state(): ConnectionState
    {
        return $this->connection->state();
    }

    /**
     * Sends a request and resolves with the first reply message. One timeout bounds the whole request.
     * While a reconnect is in flight it first waits for it within its timeout
     * ({@see NatsOptions::$waitForReconnect}).
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
        return $this->connection->request($subject, $payload, $timeoutMs, $cancellation);
    }

    /**
     * Sends a request with headers and resolves with the first reply message. One timeout bounds the
     * whole request. While a reconnect is in flight it first waits for it within that timeout
     * ({@see NatsOptions::$waitForReconnect}).
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
        return $this->connection->requestWithHeaders($subject, $payload, $headers, $timeoutMs, $cancellation);
    }

    /**
     * Sends one request and collects multiple replies (scatter-gather), terminating on the first of:
     * {@see $maxResponses} replies, a no-responders sentinel, the per-message {@see $stallMs} gap, or
     * the total timeout. Mirrors nats.go `RequestMany` / nats.java `Connection.requestMany`. While a
     * reconnect is in flight it first waits for it within the total timeout
     * ({@see NatsOptions::$waitForReconnect}).
     *
     * @param array<string,string>|null $headers Optional request headers (null = plain request).
     * @param int|null $maxResponses Stop after this many replies (null = time-bounded only).
     * @param int|null $totalTimeoutMs Overall budget in ms (null = configured request timeout).
     * @param int|null $stallMs Stop once this long passes with no new reply (null = disabled).
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
        return $this->connection->requestMany($subject, $payload, $headers, $maxResponses, $totalTimeoutMs, $stallMs, $cancellation);
    }

    /**
     * Returns server capabilities advertised during the INFO handshake.
     */
    public function serverInfo(): ?ServerInfo
    {
        return $this->connection->serverInfo();
    }

    /**
     * The server URL currently connected to, or null when not connected.
     */
    public function connectedUrl(): ?string
    {
        return $this->connection->connectedUrl();
    }

    /**
     * Cluster endpoints discovered from the server's INFO `connect_urls`.
     *
     * @return list<string>
     */
    public function discoveredServers(): array
    {
        return $this->connection->discoveredServers();
    }

    /**
     * The server's maximum accepted payload size, or null when unknown.
     */
    public function maxPayload(): ?int
    {
        return $this->connection->maxPayload();
    }

    /**
     * A snapshot of connection traffic counters.
     */
    public function statistics(): ConnectionStats
    {
        return $this->connection->statistics();
    }

    /**
     * The runtime options this client was constructed with.
     */
    public function options(): NatsOptions
    {
        return $this->options;
    }

    /**
     * Measures the round-trip time to the server (PING/PONG), in seconds. While a reconnect is in flight
     * it first waits for it within the request timeout ({@see NatsOptions::$waitForReconnect}); the wait
     * is not part of the measured time.
     *
     * @return Future<float>
     */
    public function rtt(): Future
    {
        return $this->connection->rtt();
    }

    /**
     * Returns a JetStream API context bound to this client instance.
     */
    public function jetStream(): JetStreamContext
    {
        if ($this->jetStreamContext === null) {
            $this->jetStreamContext = new JetStreamContext($this);
        }

        return $this->jetStreamContext;
    }

    /**
     * Creates a services-framework runtime bound to this client.
     *
     * @param array<string,string> $metadata
     */
    public function service(string $name, string $version, ?string $description = null, array $metadata = []): Service
    {
        return new Service($this, $name, $version, $description, $metadata);
    }
}
