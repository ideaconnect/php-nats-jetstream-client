<?php

declare(strict_types=1);

namespace IDCT\NATS\Transport;

use Amp\Socket\Certificate;
use Amp\Socket\ClientTlsContext;
use IDCT\NATS\Connection\NatsOptions;

/**
 * Shared assembly of the Amp {@see ClientTlsContext} from {@see NatsOptions}, used by both the TCP
 * ({@see AmpSocketTransport}) and WebSocket ({@see WebSocketTransport}) transports, whose copies were
 * otherwise identical.
 */
trait AssemblesClientTlsContext
{
    /**
     * Builds the client TLS context for a connection. A caller-supplied $options->tlsContext is returned
     * verbatim (escape hatch for in-memory PEM, ALPN, or custom verification); otherwise the peer name
     * (falling back to $fallbackPeerName when tlsPeerName is unset), peer verification, CA file, and
     * client certificate are applied from the options.
     */
    private function clientTlsContextFromOptions(NatsOptions $options, string $fallbackPeerName): ClientTlsContext
    {
        if ($options->tlsContext !== null) {
            return $options->tlsContext;
        }

        $peerName = $options->tlsPeerName;
        if ($peerName === null || $peerName === '') {
            $peerName = $fallbackPeerName;
        }

        $tlsContext = new ClientTlsContext($peerName);

        if (!$options->tlsVerifyPeer) {
            $tlsContext = $tlsContext->withoutPeerVerification();
        }

        if ($options->tlsCaFile !== null && $options->tlsCaFile !== '') {
            $tlsContext = $tlsContext->withCaFile($options->tlsCaFile);
        }

        if ($options->tlsCertFile !== null && $options->tlsCertFile !== '') {
            $tlsContext = $tlsContext->withCertificate(new Certificate(
                $options->tlsCertFile,
                $options->tlsKeyFile,
                $options->tlsKeyPassphrase,
            ));
        }

        return $tlsContext;
    }
}
