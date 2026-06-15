<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\ObjectStore;

/**
 * Options for {@see ObjectStoreBucket::watch()}, mirroring the nats.go / nats.java Object Store watch
 * option matrix.
 *
 * Delivery-policy precedence (highest first): {@see $includeHistory}, {@see $updatesOnly}. With none set,
 * the watcher first replays the current metadata of every existing object (last-per-subject) and then
 * streams live updates - the reference-client ObjectStore.Watch default.
 *
 * Note: calling `watch()` with `$options = null` keeps the original updates-only behavior
 * (deliver_policy=new) for backward compatibility; pass an instance of this class (even with no flags
 * set) to opt into the reference "snapshot then follow" semantics.
 */
final class ObjectStoreWatchOptions
{
    /**
     * @param bool $updatesOnly    Deliver only objects changed after the watch starts (deliver_policy=new);
     *                             no existing objects are replayed.
     * @param bool $includeHistory Deliver every metadata revision of each object (deliver_policy=all),
     *                             not just its current state.
     */
    public function __construct(
        public readonly bool $updatesOnly = false,
        public readonly bool $includeHistory = false,
    ) {}

    /**
     * Resolves the consumer config fragment (delivery policy) implied by these options.
     *
     * @return array<string,mixed>
     */
    public function toConsumerConfig(): array
    {
        $config = ['ack_policy' => 'none'];

        if ($this->includeHistory) {
            $config['deliver_policy'] = 'all';
        } elseif ($this->updatesOnly) {
            $config['deliver_policy'] = 'new';
        } else {
            // Reference ObjectStore.Watch default: current metadata of every existing object, then follow.
            $config['deliver_policy'] = 'last_per_subject';
        }

        return $config;
    }
}
