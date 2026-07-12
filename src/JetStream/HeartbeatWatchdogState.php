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

    /**
     * True while a recreate is in flight, serializing the two fibers that can trigger one for an ordered
     * consumer - the message-dispatch handler (sequence-gap / tail-gap) and the watchdog timer (onMiss).
     * The dispatch handler is serialized against itself by NatsConnection's per-sid dispatch guard, but
     * the watchdog runs in its own timer fiber the per-sid guard does not cover, so without this flag a
     * frame arriving after the watchdog computed opt_start_seq (from a now-stale lastStreamSeq) but
     * before its first await could drive a second concurrent CONSUMER.CREATE and orphan an ephemeral
     * consumer. Only the ordered recreate closure reads/writes it (the caller-owned push path never
     * recreates); it must be cleared in a finally so a legitimately-needed later recreate is not blocked.
     */
    public bool $recreateInFlight = false;

    /**
     * The deliver-inbox subscription id an ordered consumer guards, set once the subscription is
     * established. On a TERMINAL recreate failure the recreate closure unsubscribes it so the dead
     * consumer's inbox stops receiving traffic (including filtered orphan/stale frames) (#122). Null
     * until armed and for caller-owned push consumers (which never recreate).
     */
    public ?int $deliverSid = null;

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
