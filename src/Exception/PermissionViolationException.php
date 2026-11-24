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
 * Thrown by request(), requestWithHeaders() and requestMany() (one that has collected no reply), and so by the
 * JetStream API calls and JetStream publishes made through them, as soon as the -ERR is read, whichever fiber's read
 * meets it. Before, request() and requestWithHeaders() waited out their timeout and failed with a
 * {@see TimeoutException}, and requestMany() returned an empty list. Not a {@see ConnectionException}: the connection is
 * fine, and reconnecting would not help. The -ERR still reaches the error listener as before.
 *
 * Thrown as well by fetchBatch() and fetchNext() (one that has received no message), whose pull to
 * "$JS.API.CONSUMER.MSG.NEXT.<stream>.<consumer>" the server refused, at once, where they used to end at their expiry
 * with the {@see JetStreamException} (408) of an empty pull (#260). Not a JetStreamException, so a caller that reads a
 * 408 as "nothing to fetch" does not take it for one. The pull consumer and directGetBatch() publish with an inbox of
 * their own and are not covered: a refused pull of theirs still ends at its expiry.
 * The -ERR names the subject and not the publish: one left unread from an earlier refused publish to the subject
 * refuses a later request or pull to it as well, whatever the server did with that request's own publish. It is
 * matched as the server writes it, so one the client cannot match to the subject it published to - one the account
 * maps to another subject, or one with a character Go writes as a \u escape (#259) - throws nothing: the request ends
 * with its TimeoutException and the pull with its 408, as before.
 */
final class PermissionViolationException extends NatsException
{
    /**
     * @param string $subject The subject the server refused the publish to.
     */
    public function __construct(
        string $message = '',
        int $code = 0,
        ?\Throwable $previous = null,
        private readonly string $subject = '',
    ) {
        parent::__construct($message, $code, $previous);
    }

    /**
     * The subject the server refused the publish to.
     */
    public function getSubject(): string
    {
        return $this->subject;
    }
}
