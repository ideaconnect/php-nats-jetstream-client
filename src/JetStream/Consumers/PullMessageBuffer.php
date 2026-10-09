<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Consumers;

use IDCT\NATS\Core\NatsMessage;

/**
 * Messages a run of the pipelined pull engine ({@see \IDCT\NATS\JetStream\JetStreamContext::consumePipelined()}) has
 * received and not yet handed to its handler, with the place of the delivery under way: what a pull in flight holds
 * ({@see PullInFlight}), and what an infinite run received that no pull in flight had room for ({@see PullOverflow},
 * #187). The non-suspending pull-inbox router appends to {@see $buffer}; only the engine's fiber hands it to the
 * handler, which empties it.
 *
 * @internal Not part of the supported public API.
 */
abstract class PullMessageBuffer
{
    /**
     * Data messages received, not yet delivered to the handler, in the order they arrived.
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

    /** The messages of {@see $buffer} the handler has not got yet (#207). */
    public function undelivered(): int
    {
        return count($this->buffer) - $this->handedOver;
    }
}
