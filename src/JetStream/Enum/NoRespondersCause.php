<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Enum;

/**
 * Why a pull from a durable consumer ended with status 503 (no responders), as
 * {@see \IDCT\NATS\JetStream\JetStreamContext::diagnoseNoResponders()} tells it.
 */
enum NoRespondersCause: string
{
    /** The server answered that the consumer does not exist (404, err_code 10014). */
    case ConsumerNotFound = 'consumer_not_found';

    /** The server answered that the consumer's stream does not exist (404, err_code 10059). */
    case StreamNotFound = 'stream_not_found';

    /**
     * The server answered with the consumer: it exists, and nothing served its pulls for a moment, as during a
     * leader election in a cluster.
     */
    case ConsumerFound = 'consumer_found';

    /**
     * The server answered with another JetStream error, or had no responder for the question either: JetStream is
     * not enabled, not for the account, or temporarily unavailable.
     */
    case JetStreamError = 'jetstream_error';

    /**
     * Nothing answered, and the connection the pull came on left the client's use meanwhile: the client closed it,
     * is recovering it, has replaced it or is draining it, or its server announced lame duck mode. The pull met a
     * server that is going away, which stops its consumers before it closes the connection; the next pull goes out
     * on the connection that follows.
     */
    case ServerGoingAway = 'server_going_away';

    /**
     * Nothing answered within the timeout, on a connection that stays: a user allowed to pull but not to ask for
     * consumer info (the server reports a permissions violation and answers nothing), or a server too busy to answer.
     */
    case NoAnswer = 'no_answer';
}
