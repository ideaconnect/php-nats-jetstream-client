<?php

declare(strict_types=1);

namespace IDCT\NATS\Transport;

/**
 * Optional capability a transport may implement so the connection layer can verify that TLS was
 * actually established before sending credentials.
 *
 * Kept separate from {@see TransportInterface} so adding it does not break third-party transport
 * implementations; the connection layer uses an instanceof check and does not apply the
 * TLS fail-safe to transports that do not implement it.
 */
interface TlsAwareTransportInterface extends TransportInterface
{
    /**
     * Returns true only once a TLS handshake has completed on the current socket.
     */
    public function tlsActive(): bool;
}
