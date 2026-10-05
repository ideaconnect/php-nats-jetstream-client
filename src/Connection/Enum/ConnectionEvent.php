<?php

declare(strict_types=1);

namespace IDCT\NATS\Connection\Enum;

/**
 * Lifecycle transitions reported to a {@see \IDCT\NATS\Connection\NatsOptions::$connectionListener}.
 *
 * Mirrors the event sets of nats.go (Connect/Disconnect/Reconnect/Closed/DiscoveredServers/LameDuck
 * handlers) and nats.java (`ConnectionListener.Events`).
 */
enum ConnectionEvent
{
    /**
     * The initial connection handshake completed and the connection is open. When a reconnect completed a
     * failed initial connect, it is announced like a Reconnected, once that reconnect is over.
     */
    case Connected;

    /**
     * The transport was lost; the client will attempt to reconnect (if enabled). Not announced for a
     * connection the listener had not been told of yet - its Reconnected, or the Connected of a failed initial
     * connect a reconnect completed, still to come: the listener hears of the next connection instead.
     */
    case Disconnected;

    /**
     * A reconnect attempt succeeded and subscriptions were replayed. Announced once the reconnect is over, and
     * only while the connection it announces is still open: a listener called with it always finds the
     * connection Open. Announced from the event loop when another Connected or Reconnected listener call is
     * running - typically the one whose operation had to reconnect - so that listener calls do not nest; the
     * messages the reconnect read can then reach subscription handlers first. All of this holds for the
     * Connected of a failed initial connect that a reconnect completed as well.
     */
    case Reconnected;

    /**
     * The connection was closed for good: by disconnect(), by drain() - once, when the drain is over -
     * or because connecting or reconnecting gave up (attempts exhausted, credentials refused, or the
     * connection was lost with reconnect disabled).
     */
    case Closed;

    /** The server advertised additional cluster endpoints in an async INFO (`connect_urls`). */
    case DiscoveredServers;

    /** The server entered lame-duck mode (`ldm`) and is shutting down gracefully. */
    case LameDuck;
}
