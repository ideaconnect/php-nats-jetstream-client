<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Consumers;

use IDCT\NATS\Exception\JetStreamException;

/**
 * Immutable snapshot of a {@see PullConsumerIterator}'s configuration handed to the pipelined pull
 * engine ({@see \IDCT\NATS\JetStream\JetStreamContext::consumePipelined()}) at the start of a run.
 *
 * It decouples the engine (which lives on JetStreamContext, the only holder of the client handle)
 * from the mutable iterator: everything the engine needs to shape pull requests and drive the
 * lifecycle is frozen here at handle() time, EXCEPT the pin id and the stop/drain flags, which stay
 * live through {@see PullPipelineControl} so a handler can mutate them mid-run (#120).
 *
 * @internal Not part of the supported public API.
 */
final class PullPipelineConfig
{
    /**
     * @param int $batch Messages requested per pull (batch=1 still pipelines across $depth pulls).
     * @param int $expiresMs Server-side pull expiry (ms); the engine's per-pull deadline is this + 1000ms.
     * @param int $depth Max concurrent in-flight pulls in infinite, ungrouped, non-idle steady state.
     * @param int|null $iterations Finite pull count (null = infinite). A finite run is strictly serial.
     * @param bool $noWait Whether pulls carry no_wait (immediate empties need idle-backoff pacing, #153).
     * @param bool $grouped Whether an ADR-42 priority group is set (gates pin capture + serial-until-pinned).
     * @param array<string,mixed> $pullFields Snapshot of the iterator's buildPull() output; the engine
     *        overrides the dynamic `id` (pin) field per issue from the live {@see PullPipelineControl}.
     * @param int|null $idleHeartbeatNs Requested ADR-13 idle heartbeat (ns), or null. A route-wide miss
     *        is non-terminal: the per-pull deadline bounds a silent pull instead (#120 FIX2).
     * @param (\Closure(JetStreamException):void)|null $onError Diagnostics callback fired once on a
     *        non-routine termination (#63).
     */
    public function __construct(
        public readonly int $batch,
        public readonly int $expiresMs,
        public readonly int $depth,
        public readonly ?int $iterations,
        public readonly bool $noWait,
        public readonly bool $grouped,
        public readonly array $pullFields,
        public readonly ?int $idleHeartbeatNs,
        public readonly ?\Closure $onError,
    ) {}
}
