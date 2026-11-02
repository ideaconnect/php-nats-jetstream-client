<?php

declare(strict_types=1);

namespace IDCT\NATS\Transport;

use Amp\Socket\ResourceSocket;
use Amp\Socket\Socket;

/**
 * The check behind {@see PeerCloseAwareTransportInterface::peerClosed()} for a transport over an Amp socket.
 */
trait ChecksForAPeerClose
{
    /**
     * Whether the peer of $socket has closed it with nothing left unread: a peek at the socket finds the end of the
     * stream. The peek reads nothing (MSG_PEEK) and does not wait, the socket being non-blocking: no bytes yet and
     * bytes waiting both answer false. At the socket level, below TLS, so over TLS the close_notify alert the peer
     * sends ahead of its close is such a byte. About half a microsecond, one system call.
     */
    private static function peerHasClosed(?Socket $socket): bool
    {
        $resource = $socket instanceof ResourceSocket ? $socket->getResource() : null;

        return \is_resource($resource) && @\stream_socket_recvfrom($resource, 1, \STREAM_PEEK) === '';
    }
}
