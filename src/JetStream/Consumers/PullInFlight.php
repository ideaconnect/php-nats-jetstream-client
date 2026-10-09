<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Consumers;

/**
 * Mutable per-in-flight-pull state the pipelined pull engine tracks for each outstanding
 * CONSUMER.MSG.NEXT request, keyed by the reply-suffix token routed to it.
 *
 * The non-suspending pull-inbox router fills {@see $buffer}/{@see $received}/{@see $terminalCode} as
 * frames arrive; the engine fiber drains the buffer to the handler and retires the pull. Generalizes
 * fetchBatch()'s single-inbox `$messages`/`$terminalStatus`/deadline bookkeeping to N in-flight
 * pulls (#120). It is the run's count of a request, not the server's: the server can hold a request the
 * run no longer counts, after a reconnect, and what that request brings goes to the oldest pull still
 * open, or to the run's {@see PullOverflow} (#187).
 *
 * @internal Not part of the supported public API.
 */
final class PullInFlight extends PullMessageBuffer
{
    /** Count of data messages received on this pull; reaching {@see $batch} retires it as full. */
    public int $received = 0;

    /**
     * Whether the pull is complete: a full batch was received, or a terminal status frame arrived. Also set when the
     * run hands the buffer over right before it ends with a failure (#197), when an infinite run hands it over after a
     * frame ended the connection (#210), when the client's drain() asks the run for its hand-over (#207), or when an
     * infinite run ends it after a reconnect ({@see $endedByReconnect}, #187), so that the router attributes nothing
     * more to the pull while the handler runs. The infinite run clears it again on a pull still open that it keeps in
     * flight, emptied, while the reconnect is still under way, as the pulls in flight after an EOF stay open.
     */
    public bool $done = false;

    /**
     * Whether an infinite run ended the pull because the connection reconnected after it was registered (#187). The
     * run cannot tell whether the server still holds the request: a server that restarted forgot it, one that outlived
     * the connection serves it once the reconnect subscribes the run's inbox again, and a pull written from the
     * reconnect buffer reaches the new connection. So the pull is {@see $done}, the router attributing nothing more to
     * it, the retire phase hands what it received to the handler in issue order, as for any pull, and a pull that
     * brought nothing leaves the run unclassified: a status it got before the reconnect neither ends the run nor
     * reaches onError, nor counts as an idle retire. What the request still brings goes to the pulls the run issues on
     * the new connection, or to the run's {@see PullOverflow}.
     */
    public bool $endedByReconnect = false;

    /** Terminal status code (>=400) captured raw from a status frame, or null when none arrived. */
    public ?int $terminalCode = null;

    /** Terminal status description, when a terminal status frame arrived (empty otherwise). */
    public string $terminalDescription = '';

    /**
     * @param string $token Reply-subject suffix token uniquely identifying this pull within the run.
     * @param int $batch Batch size requested (the received >= batch retire threshold).
     * @param int $deadlineNs Monotonic (hrtime) deadline after which a silent pull is retired as empty.
     */
    public function __construct(
        public readonly string $token,
        public readonly int $batch,
        public readonly int $deadlineNs,
    ) {}
}
