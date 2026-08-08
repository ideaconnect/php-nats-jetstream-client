# Deferral condition is sampled after the orphan-reap awaits — a reconnect completing in that window turns a disconnect collision into a loud terminal teardown

- **Status:** FIXED (2026-08-08) — a sawNotOpen flag latches in the per-attempt catch and at the outer catch
  entry; the deferral condition is now (sawNotOpen || state() !== Open), so a reconnect completing
  during the orphan-reap awaits can no longer flip a disconnect collision into a terminal
  teardown. Pinned by `testWatchdogRecreateDeferralLatchesNotOpenObservationAcrossReapAwaits`
  (pre-fix red: terminal 'recreate failed' emitted).
- **Severity:** nit
- **Type:** race window (loud, non-silent)
- **Area:** JetStream ordered consumers — disconnect-collision deferral
- **Where:** `src/JetStream/JetStreamContext.php:1531` (`reapOrphanedConsumers` before the check
  at `:1533`), `:1513-1519` (orphan candidates appended by failed attempts)

## Problem

The deferral condition `state() !== Open` is evaluated only AFTER `reapOrphanedConsumers()` runs.
When the failed attempts appended orphan candidates, each reap iteration is an
`async()->await()` suspension (the deletes fail fast while not Open, but the await still yields
event-loop turns). If the reconnect completes in one of those turns, the check reads Open, the
deferral is skipped, and the terminal branch emits "Ordered consumer recreate failed" and tears
the watch down — for exactly the disconnect-collision episode the deferral was built to defer.

Not a defect of the latch fix (the window exists identically with or without it); the failure
mode is loud (error emitted, subscription torn down — the pre-deferral baseline behavior), and
the branch cannot be hit when the retry loop never ran (fail-fast subscribe leaves the candidate
list empty, so there is no suspension between the throw and the check).

## Suggested fix

Latch the observation during the episode instead of sampling after the reap: set a
`$sawNotOpen` flag when an attempt fails while `state() !== Open` and use
`($sawNotOpen || state() !== Open) && !$state->stopped` for the deferral decision — or simply
evaluate the deferral condition before calling `reapOrphanedConsumers()` (the reap is equally
valid in both branches).
