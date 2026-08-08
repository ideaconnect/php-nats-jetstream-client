<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\KeyValue;

/**
 * Options for {@see KeyValueBucket::watch()}, mirroring the nats.go / nats.java KV watch option matrix.
 *
 * Delivery-policy precedence (highest first): {@see $resumeFromRevision}, {@see $includeHistory},
 * {@see $updatesOnly}. When an instance of this class is supplied with none of those flags set, the
 * watcher replays the current value of every matching key (last-per-subject) and then streams live
 * updates - the reference-client default.
 *
 * Note: this last-per-subject default applies only when an instance is passed. {@see KeyValueBucket::watch()}
 * called with `$options = null` (the common form) is updates-only (deliver_policy=new) and replays nothing;
 * pass `new KeyWatchOptions()` to get the snapshot-then-follow behavior.
 */
final class KeyWatchOptions
{
    /**
     * @param bool          $includeHistory    Deliver the full history of each key (deliver_policy=all),
     *                                          not just the latest value.
     * @param bool          $updatesOnly       Deliver only updates that occur after the watch starts
     *                                          (deliver_policy=new); no initial values are replayed.
     * @param bool          $ignoreDeletes     Suppress delete/purge tombstones from the handler.
     * @param bool          $metaOnly          Deliver headers only (no value bytes); the entry value is
     *                                          empty. Useful for cheaply enumerating keys/revisions.
     * @param int|null      $resumeFromRevision Start delivery at this stream sequence
     *                                          (deliver_policy=by_start_sequence), e.g. to resume a watch.
     * @param (\Closure():void)|null $onCaughtUp Invoked once when the initial replay has caught up to the
     *                                          current end of the stream - either a delivered message
     *                                          reports num_pending = 0, or the consumer starts with nothing
     *                                          pending (empty / no-match bucket), in which case it fires
     *                                          immediately without any delivery (#99). Mirrors the
     *                                          reference "end of initial data" signal.
     * @param int|null      $idleHeartbeat      Idle-heartbeat interval in nanoseconds the watch consumer
     *                                          requests. The watch runs on an ordered consumer, so this
     *                                          interval also drives its missed-heartbeat watchdog: after
     *                                          two intervals with no inbound frame at all (data, heartbeat
     *                                          or flow control) the watch RECREATES its consumer and
     *                                          resumes just after the last delivered revision, so a silent
     *                                          or reaped consumer recovers on its own instead of hanging
     *                                          forever (#113). An error reaches the client only if every
     *                                          recreate attempt fails. A shorter interval detects the
     *                                          silence sooner at the cost of more heartbeat traffic.
     *                                          Null uses {@see KeyValueBucket::WATCH_IDLE_HEARTBEAT_NS}.
     *                                          Must be a positive integer.
     */
    public function __construct(
        public readonly bool $includeHistory = false,
        public readonly bool $updatesOnly = false,
        public readonly bool $ignoreDeletes = false,
        public readonly bool $metaOnly = false,
        public readonly ?int $resumeFromRevision = null,
        public readonly ?\Closure $onCaughtUp = null,
        public readonly ?int $idleHeartbeat = null,
    ) {
        if ($idleHeartbeat !== null && $idleHeartbeat <= 0) {
            throw new \InvalidArgumentException('idleHeartbeat must be a positive integer (nanoseconds)');
        }
    }

    /**
     * Resolves the consumer config fragment (deliver policy + headers-only) implied by these options.
     *
     * @return array<string,mixed>
     */
    public function toConsumerConfig(): array
    {
        $config = ['ack_policy' => 'none'];

        if ($this->resumeFromRevision !== null) {
            $config['deliver_policy'] = 'by_start_sequence';
            $config['opt_start_seq'] = $this->resumeFromRevision;
        } elseif ($this->includeHistory) {
            $config['deliver_policy'] = 'all';
        } elseif ($this->updatesOnly) {
            $config['deliver_policy'] = 'new';
        } else {
            $config['deliver_policy'] = 'last_per_subject';
        }

        if ($this->metaOnly) {
            $config['headers_only'] = true;
        }

        if ($this->idleHeartbeat !== null) {
            $config['idle_heartbeat'] = $this->idleHeartbeat;
        }

        return $config;
    }
}
