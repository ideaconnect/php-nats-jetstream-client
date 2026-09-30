<?php

declare(strict_types=1);

namespace IDCT\NATS\Exception;

/**
 * Raised under {@see \IDCT\NATS\Connection\Enum\SlowConsumerPolicy::Error} when a message arrives for a
 * subscription whose queue is full - or whose SubscriptionQueue's polling buffer is: that message is
 * dropped, and core NATS does not resend it. The connection itself is fine - the subscriber is not keeping
 * up. A ConnectionException, as a full subscription queue's overflow always was, so existing handlers still
 * catch it.
 */
final class SlowConsumerException extends ConnectionException
{
    /**
     * @param int $sid The subscription whose queue overflowed.
     */
    public function __construct(public readonly int $sid)
    {
        parent::__construct('Subscription queue overflow for sid ' . $sid);
    }
}
