<?php

declare(strict_types=1);

namespace IDCT\NATS\Exception;

/**
 * Thrown when a JetStream request fails because the connected NATS server is too old to support the
 * requested feature (for example creating a stream with `allow_atomic` against a pre-2.12 server).
 *
 * It is a {@see JetStreamException}, so existing `catch (JetStreamException)` handlers still catch it;
 * catch this type specifically to detect "feature not available on this server version" distinctly.
 * A server older than a version-gated stream/consumer config field drops the field unless it runs in
 * strict mode (the default from 2.12), so the stream and consumer create/update calls check the
 * INFO-advertised server version before sending (nothing was sent) and the server's reply after it.
 * A reply check failure means the request was sent: either the returned config lacks the field (the
 * stream or consumer exists without the feature, and may have existed before the call, since a create
 * returns an existing one whose config matches) or the reply carries no config (its state is unknown).
 * Inspect it before changing or deleting it (#266).
 * Atomic batch `commit()` pre-flights the version the same way, so nothing is published to a server
 * that cannot honor batch semantics (#152). A strict server's `unknown field` rejection maps here too.
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
