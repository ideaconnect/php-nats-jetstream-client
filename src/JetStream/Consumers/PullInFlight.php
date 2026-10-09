<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Consumers;

use IDCT\NATS\Core\NatsMessage;

/**
 * Mutable per-in-flight-pull state the pipelined pull engine tracks for each outstanding
 * CONSUMER.MSG.NEXT request, keyed by the reply-suffix token routed to it.
 *
 * The non-suspending pull-inbox router fills {@see $buffer}/{@see $received}/{@see $terminalCode} as
 * frames arrive; the engine fiber drains the buffer to the handler and retires the pull. Generalizes
 * fetchBatch()'s single-inbox `$messages`/`$terminalStatus`/deadline bookkeeping to N in-flight
 * pulls (#120).
 *
 * @internal Not part of the supported public API.
 */
final class PullInFlight
{
    /**
     * Data messages received on this pull, not yet delivered to the handler (drained on retire).
     *
     * @var list<NatsMessage>
     */
    public array $buffer = [];

    /**
     * How many of {@see $buffer}'s messages a delivery to the handler has handed over so far: the delivery's place in
     * the buffer while the handler runs, so that what the handler has not got yet is known ({@see undelivered()}),
     * for the drain's deadline report (#207). Back to 0 when the delivery empties the buffer.
     */
    public int $handedOver = 0;

    /** Count of data messages received on this pull; reaching {@see $batch} retires it as full. */
    public int $received = 0;

    /**
     * Whether the pull is complete: a full batch was received, or a terminal status frame arrived. Also set when the
     * run hands the buffer over right before it ends with a failure (#197), when an infinite run hands it over after a
     * frame ended the connection (#210), or when the client's drain() asks the run for its hand-over (#207), so that
     * the router attributes nothing more to the pull while the handler runs. The infinite run clears it again on a pull
     * still open that it keeps in flight, emptied, while the reconnect is still under way, as the pulls in flight after
     * an EOF stay open.
     */
    public bool $done = false;

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

    /** The messages of {@see $buffer} the handler has not got yet (#207). */
    public function undelivered(): int
    {
        return count($this->buffer) - $this->handedOver;
    }
}
