<?php

declare(strict_types=1);

namespace IDCT\NATS\Connection\Enum;

enum SlowConsumerPolicy: string
{
    /** Drop the oldest queued message and keep newer arrivals. */
    case DropOldest = 'drop_oldest';
    /** Drop the newest incoming message when queue is full. */
    case DropNewest = 'drop_newest';
    /**
     * Drop the overflowing (newest) message AND raise an error when queue capacity is exceeded. The
     * dropped message is still lost - core NATS does not resend it - but the loss is surfaced loudly, as a
     * {@see \IDCT\NATS\Exception\SlowConsumerException} naming the subscription, instead of silently.
     * The processIncoming() or readIncoming() that overflowed the queue throws it, once it has delivered
     * the rest of what it read (a fatal -ERR in the same read is thrown in its place). The client's own
     * reads report it through the error listener instead, logged at error level: an operation waiting for
     * a result of its own - a request, a flush, a fetch, a polling queue - completes unless the overflow is
     * of its own subscription (or {@see \IDCT\NATS\Connection\NatsOptions::$slowConsumerErrorsFailOperations}
     * is set), and Service::run(), the heartbeat, a reconnect and the flushes of drain() and
     * drainSubscription() always carry on. A SubscriptionQueue's own polling buffer follows the same
     * rules, and counts its drops in droppedCount(). The dropped message still counts toward
     * auto-unsubscribe allowance, exactly like DropOldest/DropNewest: the server counted it when it wrote
     * it, so an auto-unsub can complete having delivered fewer messages than its max, with the overflow
     * surfaced (#159/#112).
     */
    case Error = 'error';
}
