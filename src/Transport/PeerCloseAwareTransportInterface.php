<?php

declare(strict_types=1);

namespace IDCT\NATS\Transport;

/**
 * Optional capability a transport may implement so that the connection can tell, before a write, that the server
 * has already closed the connection.
 *
 * The connection reads the socket only while an operation waits for a reply. A close the server sends while nothing
 * reads - a restart, a rolling upgrade, a failover, `docker stop` - stays unread, and a write that waits for no
 * reply (a publish, an ACK, a NAK) is still taken by the socket and reaches no server. The connection asks this
 * before such a write, and before the write of a request, and handles a yes as a write that failed: the frame goes
 * to the reconnect buffer, or is sent again once the connection has recovered.
 *
 * Kept separate from {@see TransportInterface} so adding it does not break third-party transports; the connection
 * checks with instanceof and writes to a transport without it as before. Both built-in transports implement it.
 */
interface PeerCloseAwareTransportInterface extends TransportInterface
{
    /**
     * Whether the peer has closed the current connection with nothing it sent before the close left unread, so
     * that a write now would reach no one.
     *
     * Must return at once, read nothing a later read would miss, and answer false when it cannot tell: with no
     * connection, or with bytes still waiting to be read ahead of the close (over TLS, the server's close_notify).
     */
    public function peerClosed(): bool;
}
