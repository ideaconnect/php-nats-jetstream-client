<?php

declare(strict_types=1);

namespace IDCT\NATS\Exception;

/**
 * The connection is at its subscription limit: the server answered with "-ERR 'maximum subscriptions exceeded'".
 * The limit is the smallest of the server's max_subscriptions, the account's and the user's. A ConnectionException,
 * as these failures always were, so existing handlers still catch it, and its message is unchanged.
 *
 * It tells why a call failed, not what state the connection is in. The server sends the same -ERR when it refuses a
 * SUB beyond the limit, keeping the connection open, and when an account's limit is lowered below what the connection
 * already holds, closing the connection right behind it. The -ERR names no subscription either, so an error about a
 * reply inbox says the server may have rejected that inbox, not that it did. Whether to retry, wait for a free slot or
 * raise the limit is the caller's decision.
 *
 * Thrown, or reported to the error listener, in these cases (#242):
 * - the read that meets the -ERR, with "Server sent error frame: ..." (the heartbeat's read, a reconnect's replay and
 *   the serving loops report it instead of throwing);
 * - a request or requestMany() whose shared reply inbox was dropped for the -ERR;
 * - a JetStream pull fetch or a batched Direct Get whose reply inbox was dropped for it;
 * - a guarded subscribe whose SUB a reconnect replayed and the new server rejected for it.
 *
 * A pull consumer run still fails with a {@see JetStreamException}, as it always has; when the subscribe itself
 * reported the rejection, this exception is its previous one.
 */
final class SubscriptionLimitException extends ConnectionException {}
