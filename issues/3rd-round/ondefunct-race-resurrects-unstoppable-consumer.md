# onDefunct racing an in-flight recreate resurrects an unstoppable consumer — the cleanup never latches $state->stopped

- **Status:** FIXED (2026-08-08) — onDefunct now latches `$state->stopped = true` FIRST, before releasing the
  registry entry and issuing the best-effort delete, exactly like stopOrderedConsumer()'s closure:
  a recreate parked in its awaits when the tick fires tears its fresh instance down (existing
  stopped branches) instead of installing a consumer whose stop handle is already gone. Pinned by
  `JetStreamContextTest::testDefunctTickDuringParkedRecreateCreatePreventsInstall` (plain
  unsubscribe during a withheld CONSUMER.CREATE, tick runs onDefunct, then the reply is released:
  the resumed recreate must tear down, not install/deliver), verified red against the removed
  latch.
- **Severity:** minor
- **Type:** state-machine race / regression of the stop contract (introduced by the 2nd-round batch)
- **Area:** JetStream ordered consumers — plain-unsubscribe registry cleanup
- **Where:** `src/JetStream/JetStreamContext.php:1844-1860` (onDefunct: unset + delete, no stopped
  latch), `:1799-1803` (stop closure's latch-FIRST contract), `:3324-3335` (tick invokes onDefunct
  without consulting `recreateInFlight`), `:1514`/`:1587` (recreate's stopped gates)

## Problem

The new `onDefunct` hook releases the `orderedStops` entry and best-effort deletes the current
consumer, but unlike the stop closure it never sets `$state->stopped`. If the watchdog self-cancel
tick fires while a recreate is suspended in its awaits (create parked up to requestTimeoutMs=10s
vs a 5s default tick), the recreate later passes both stopped checks, installs the fresh instance,
and re-arms a new watchdog — a live, delivering consumer whose stop-registry entry onDefunct just
removed. `stopOrderedConsumer($initialSid)` then falls back to a plain unsubscribe of the
already-dead initial sid (a silent no-op), so the consumer is permanently unstoppable short of
disconnecting. Additionally, onDefunct's delete targets the by-ref `$consumerName`, which
mid-episode names the adopted CANDIDATE: a delete landing after the create kills the
just-installed consumer, and the newly armed watchdog recreates it two intervals later — a
self-healing loop that keeps delivering to the handler the user tried to stop.

Pre-batch, the same race resurrected the consumer but LEFT the registry entry, so
`stopOrderedConsumer` could still stop it — the batch turned a recoverable race into an
unrecoverable one.

## Failure scenario

A KV watch's recreate parks in its CONSUMER.CREATE await against a degraded JS API. The user stops
the watch via the still-documented legacy `unsubscribe($sid)`. Within ~5s the tick runs onDefunct
(entry released, delete sent for the candidate name). The parked create then succeeds; `stopped`
is still false, so the fresh instance installs and re-arms — the handler keeps receiving messages
after the unsubscribe, and `stopOrderedConsumer($sid)` is a silent no-op.

## Suggested fix

Latch `$state->stopped = true;` at the top of onDefunct (before the unset/delete), mirroring the
stop closure's latch-first contract: an in-flight recreate then tears down its fresh instance via
the existing stopped branches instead of installing it. Extend the plain-unsubscribe test with an
unsubscribe-during-parked-create arm asserting no instance is installed and no delivery resumes.
