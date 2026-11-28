<?php

declare(strict_types=1);

namespace IDCT\NATS\Exception;

/**
 * Thrown when a JetStream request fails because the NATS server that handled it is too old to support the
 * requested feature (for example creating a stream with `allow_atomic` that a pre-2.12 server dropped).
 *
 * It is a {@see JetStreamException}, so existing `catch (JetStreamException)` handlers still catch it;
 * catch this type specifically to detect "feature not available on this server version" distinctly.
 *
 * It is always decided from the server's answer, never from the version the connected server reports in
 * INFO (ADR-44, #272): a JetStream request is answered by the meta leader or by the server hosting the
 * asset, which may run another release than the connected one. $serverVersion is that INFO version, given
 * for information only. The request has been sent by the time this is thrown:
 *
 * - a stream or consumer create/update whose reply config lacks a version-gated field (the stream or
 *   consumer exists without the feature, and may have existed before the call, since a create returns an
 *   existing one whose config matches) or carries no config (its state is unknown); inspect it before
 *   changing or deleting it (#266);
 * - a strict server's `unknown field` rejection of such a field (nothing was set up);
 * - an atomic batch `commit()` whose start message the server stored as a plain publish (#130/#152);
 * - a batched Direct Get the server does not serve (feature `direct_get_batch`, #272).
 */
final class UnsupportedFeatureException extends JetStreamException
{
    /**
     * @param string      $feature         The server config field / feature that is unsupported (e.g. "allow_atomic").
     * @param string      $requiredVersion The minimum NATS server version that provides it (e.g. "2.12").
     * @param string|null $serverVersion   The version the connected server reported in INFO, if known (for information:
     *                                     it may not be the server that handled the request).
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
