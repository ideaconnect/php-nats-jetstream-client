<?php

declare(strict_types=1);

namespace IDCT\NATS\Connection\Enum;

/**
 * How {@see \IDCT\NATS\Connection\NatsConnection::retirePullInbox()} ended (#212): whether the pull consumer run's
 * inbox was fenced, so that everything the server sent it before its UNSUB was routed to the run, or only released.
 * Callers and tests tell a completed fence from best-effort release by it, rather than mistake a caught timeout for
 * successful synchronization.
 *
 * @internal For the pull consumer engine; not part of the supported API.
 */
enum InboxRetirement: string
{
    /**
     * The connection was Open: UNSUB and PING went out in one write, and the PONG answering that PING arrived, every
     * message the server sent the inbox before it had the UNSUB routed to the run first.
     */
    case Fenced = 'fenced';

    /**
     * The connection was Draining: the client's drain() owned the UNSUB and its flush, and the drain asked the run for
     * its hand-over, which says that its flush phase is over (not necessarily that its PONG arrived).
     */
    case DrainFlushed = 'drain-flushed';

    /**
     * Released without a completed fence: the cleanup budget ran out, the run was stopped, the connection was not open
     * (a reconnect under way, closed), the UNSUB or the read failed, or a drain's readiness did not come in time. What the
     * local queue held was still routed to the run unless a stop or a close that discards came first; what the server
     * sent after that may be lost.
     */
    case Released = 'released';

    /** Nothing was registered for the sid and no UNSUB was owed for it. */
    case Gone = 'gone';
}
