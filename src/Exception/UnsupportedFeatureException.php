<?php

declare(strict_types=1);

namespace IDCT\NATS\Exception;

/**
 * Thrown when a JetStream request fails because the connected NATS server is too old to support the
 * requested feature (for example creating a stream with `allow_atomic` against a pre-2.12 server).
 *
 * It is a {@see JetStreamException}, so existing `catch (JetStreamException)` handlers still catch it;
 * catch this type specifically to detect "feature not available on this server version" distinctly.
 * The detection is reactive - it is derived from the server's own error response, not from a
 * per-request version probe.
 */
final class UnsupportedFeatureException extends JetStreamException
{
    /**
     * @param string      $feature         The server config field / feature that is unsupported (e.g. "allow_atomic").
     * @param string      $requiredVersion The minimum NATS server version that provides it (e.g. "2.12").
     * @param string|null $serverVersion   The version the connected server reported, if known.
     */
    public function __construct(
        public readonly string $feature,
        public readonly string $requiredVersion,
        public readonly ?string $serverVersion,
        string $message,
        int $code = 0,
        ?\Throwable $previous = null,
        ?int $errCode = null,
    ) {
        parent::__construct($message, $code, $previous, $errCode);
    }
}
