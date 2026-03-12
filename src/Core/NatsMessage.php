<?php

declare(strict_types=1);

namespace IDCT\NATS\Core;

/**
 * Immutable inbound NATS message representation.
 */
final class NatsMessage
{
    /**
     * Represents a normalized delivery passed to user subscription handlers.
     *
     * @param string $subject Subject that matched the subscription and carried this message.
     * @param int $sid Subscription ID assigned by this client for the matching SUB command.
     * @param string|null $replyTo Optional reply subject set by publisher for request/reply flows.
     * @param string $payload Message payload bytes as decoded by protocol parser.
     * @param string|null $rawHeaders Raw NATS/1.0 header block for HMSG frames, including trailing CRLF section.
     */
    public function __construct(
        public readonly string $subject,
        public readonly int $sid,
        public readonly ?string $replyTo,
        public readonly string $payload,
        public readonly ?string $rawHeaders = null,
    ) {
    }
}
