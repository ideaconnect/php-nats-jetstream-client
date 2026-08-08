# Draining-branch publish writes get a fresh full requestTimeoutMs each — drain() can overshoot its documented budget by K × 10 s

- **Status:** FIXED (2026-08-08) — drain() records its deadline on the instance; the Draining branch of
  writePublishFrame() bounds handler publishes by the drain's REMAINING budget, and
  drainPendingForSid() checks the deadline between deliveries (each pass's head delivery stays
  exempt to preserve the pinned post-deadline head-delivery contract in
  NatsConnection_1MutationTest — the bound stays hard: at most one head per pass, its publishes
  clamped to the expired remainder). Pinned by
  `DrainFlushBoundedWriteTest::testDrainBudgetBoundsSerialHandlerPublishesAcrossBacklog`, verified
  red pre-fix both fully disabled (9.62s vs the 1.4s bound) and with only the loop check disabled.
- **Severity:** minor
- **Type:** blocked flow (bounded, but far beyond the documented bound) / contract mismatch
- **Area:** connection drain
- **Where:** `src/Connection/NatsConnection.php:1195` (Draining branch of `writePublishFrame`),
  `:812`/`:909` (local drain deadline, checked only between passes),
  `:3855` (`drainPendingForSid` delivery loop, no deadline consult)

## Problem

The Draining branch of `writePublishFrame()` uses

```php
$this->writeBounded($frame, new TimeoutCancellation(max(0.1, $this->options->requestTimeoutMs / 1000)));
```

— a fresh, full request-timeout budget **per publish** — while drain()'s own writes use
`remainingBudgetCancellation($drainDeadline)`. The branch comment claims parity ("Bounded like
drain()'s own writes"), but the bound used is not the drain budget: the deadline is a local
variable inside `drain()` (no instance property exists), it is consulted only **between**
`drainAllPending` passes, and `drainPendingForSid`'s `while (!$queue->isEmpty())` loop never
checks it.

## Failure scenario

Transport wedges on backpressure while drain() delivers a backlog of K messages to a handler that
publishes an ack per message and contains its own errors (a common robust-handler pattern): each
ack publish blocks the full `requestTimeoutMs` (default 10 s) before its `TimeoutException`, and
the deadline check only runs after the whole pass. drain() takes ~deadline + K × 10 s (K=30 →
5 minutes) instead of the documented single ~`requestTimeoutMs` bound (#149) the round-1 fix
claims to enforce. Even a non-catching handler overshoots the budget by one full request timeout.

## Suggested fix

Record the drain deadline on the instance while state is Draining and use
`remainingBudgetCancellation($drainDeadline)` in the Draining branch of `writePublishFrame`
(falling back to the current fixed bound when no drain is active), and/or check the deadline
inside `drainPendingForSid`'s loop while Draining.
