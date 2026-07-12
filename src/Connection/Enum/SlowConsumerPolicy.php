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
     * dropped message is still lost - core NATS does not resend it - but the loss is surfaced loudly
     * (thrown exception, plus droppedCount()/the error listener on the polling queue) instead of
     * silently. The dropped message still counts toward auto-unsubscribe allowance, exactly like
     * DropOldest/DropNewest: the server counted it when it wrote it, so an auto-unsub can complete
     * having delivered fewer messages than its max, with the overflow surfaced (#159/#112).
     */
    case Error = 'error';
}
