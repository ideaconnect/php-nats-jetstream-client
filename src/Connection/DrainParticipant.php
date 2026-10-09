<?php

declare(strict_types=1);

namespace IDCT\NATS\Connection;

use Amp\Future;

/**
 * What a {@see NatsConnection::drain()} waits for besides the connection's own backlog: one of the library's operations
 * that holds messages the connection has already handed it and a handler has not got yet (#207). The JetStream pull
 * consumer engine is one: it buffers each message into the pull it belongs to and hands a pull's messages to the
 * handler only when it retires the pull, so a drain that delivered only the connection's own queues closed the
 * connection under what the pulls held. The handler never got those messages, and the server, which counted them as
 * delivered, sent them again only after the ack wait, or never on a consumer without acks.
 *
 * The drain asks each participant once its flush is done, while the connection is Draining: the server has stopped
 * delivering on the subscriptions the drain unsubscribed, and the flush has read what it sent before their UNSUBs. The
 * drain then waits for each participant's hand-over within its single budget, as it waits for its own backlog, and the
 * handlers' acks and replies go out meanwhile. A participant is registered with
 * {@see NatsConnection::addDrainParticipant()} for as long as it can hold such messages, and removed with
 * {@see NatsConnection::removeDrainParticipant()}.
 *
 * @internal Low-level mechanism for the library's own operations; not part of the supported API.
 */
interface DrainParticipant
{
    /**
     * Asks the participant to hand over what it holds. Called at most once per drain, from the drain's fiber, and must
     * not suspend: the participant only records the request and wakes whatever waits on its behalf. The future
     * completes once the hand-over is over, however it ended (everything handed over, a stop, a handler that threw, the
     * rest discarded once the drain's budget ran out, or a disconnect() came), and it never errors, so that a drain
     * never waits on it past that.
     *
     * @return Future<null>
     */
    public function handOver(): Future;

    /**
     * How many messages the participant still holds and has not handed to a handler: what a drain whose budget runs
     * out before the hand-over is over names in its "drain deadline exceeded" report, as it names its own backlog.
     */
    public function undelivered(): int;
}
