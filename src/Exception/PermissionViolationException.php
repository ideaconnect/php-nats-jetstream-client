<?php

declare(strict_types=1);

namespace IDCT\NATS\Exception;

/**
 * The server refused the publish of a request for lack of permission: it answered with
 * "-ERR 'Permissions Violation for Publish to "<subject>"'" and kept the connection open. No responder received the
 * request, so nothing it asked for was done: a JetStream API call made no change, and a JetStream publish stored
 * nothing. Permissions belong to the connection's user, so a request to the same subject on this connection is refused
 * again until they change.
 *
 * Thrown by request(), requestWithHeaders() and requestMany(), and so by every JetStream API call and JetStream publish,
 * as soon as the -ERR is read, whichever fiber's read meets it. Before, the request waited out its timeout and failed
 * with a {@see TimeoutException}. Not a {@see ConnectionException}: the connection is fine, and reconnecting would not
 * help. The -ERR still reaches the error listener as before.
 */
final class PermissionViolationException extends NatsException
{
    /**
     * @param string $subject The subject the server refused the publish to.
     */
    public function __construct(
        public readonly string $subject,
        string $message,
        int $code = 0,
        ?\Throwable $previous = null,
    ) {
        parent::__construct($message, $code, $previous);
    }
}
