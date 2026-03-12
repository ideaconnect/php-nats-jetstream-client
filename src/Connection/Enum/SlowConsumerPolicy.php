<?php

declare(strict_types=1);

namespace IDCT\NATS\Connection\Enum;

enum SlowConsumerPolicy: string
{
    /** Drop the oldest queued message and keep newer arrivals. */
    case DropOldest = 'drop_oldest';
    /** Drop the newest incoming message when queue is full. */
    case DropNewest = 'drop_newest';
    /** Raise an error when queue capacity is exceeded. */
    case Error = 'error';
}
