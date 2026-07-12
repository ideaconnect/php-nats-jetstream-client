<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream;

/**
 * Mutable last-frame-arrival tracker shared between a JetStream push/ordered subscription handler and
 * its idle-heartbeat watchdog (#113).
 *
 * The subscription handler refreshes {@see $lastActivityNs} (monotonic hrtime) on EVERY inbound frame
 * - data, status-100 heartbeat, or flow-control - and clears {@see $notified}. The watchdog reads
 * {@see $lastActivityNs} to decide whether the consumer stopped delivering and, on a miss, invokes
 * {@see $onMiss} (recreate for an ordered consumer; an error-listener notification for a caller-owned
 * push consumer) at most once per silence episode via {@see $notified}.
 *
 * The handler holds the only strong reference to this object, so when the subscription is dropped the
 * object becomes collectible and the weakly-referencing watchdog timer stops guarding a dead
 * subscription (mirrors the #126 ping-timer teardown).
 *
 * @internal Not part of the supported API.
 */
final class HeartbeatWatchdogState
{
    /** @var \Closure():void Invoked by the watchdog on a missed-heartbeat episode; a no-op until armed. */
    public \Closure $onMiss;

    /** True once the current silence episode has been surfaced; cleared by the next inbound frame. */
    public bool $notified = false;

    public function __construct(public int $lastActivityNs)
    {
        $this->onMiss = static function (): void {};
    }

    /**
     * Records that an inbound frame just arrived: rearms the silence window (monotonic hrtime) and
     * clears the surfaced-episode latch so a fresh stall can be reported later.
     */
    public function touch(): void
    {
        $this->lastActivityNs = hrtime(true);
        $this->notified = false;
    }
}
