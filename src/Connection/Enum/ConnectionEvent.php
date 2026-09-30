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
    /** The initial connection handshake completed and the connection is open. */
    case Connected;

    /** The transport was lost; the client will attempt to reconnect (if enabled). */
    case Disconnected;

    /** A reconnect attempt succeeded and subscriptions were replayed. */
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
