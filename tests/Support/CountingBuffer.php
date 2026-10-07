<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Core\SubscriptionQueue;

/**
 * A SubscriptionQueue's message buffer that counts how often the queue checks whether it is empty: how often a poll
 * looked. next() checks the buffer as it starts and after each read, before and after its pause, and a poll parked
 * on a read that waits does not check it at all, so the count tells a poll that waits from one that looks again
 * every millisecond without timing anything.
 *
 * Installed with {@see install()}, which puts it in place of the queue's own buffer, with whatever that buffer holds.
 * Sibling of {@see CountingCancellation}, which counts the looks of an operation that takes a cancellation.
 *
 * @extends \SplQueue<NatsMessage>
 */
final class CountingBuffer extends \SplQueue
{
    /** How often the queue checked whether this buffer is empty. */
    public int $checks = 0;

    /** Puts a counting buffer in place of $queue's own, carrying over the messages it holds. */
    public static function install(SubscriptionQueue $queue): self
    {
        $property = new \ReflectionProperty(SubscriptionQueue::class, 'messages');
        $buffer = new self();
        $current = $property->getValue($queue);
        if ($current instanceof \SplQueue) {
            foreach ($current as $message) {
                if ($message instanceof NatsMessage) {
                    $buffer->enqueue($message);
                }
            }
        }

        $property->setValue($queue, $buffer);

        return $buffer;
    }

    public function isEmpty(): bool
    {
        $this->checks++;

        return parent::isEmpty();
    }
}
