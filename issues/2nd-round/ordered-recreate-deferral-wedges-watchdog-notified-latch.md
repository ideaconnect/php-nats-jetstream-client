# Disconnect-collision deferral never clears the watchdog `notified` latch — a watchdog-triggered recreate that collides with a disconnect stalls the watch permanently, silently

- **Status:** FIXED (2026-08-08) — the deferral branch now clears `$state->notified` before
  returning, so the watchdog genuinely re-fires two idle intervals after the connection is Open
  again (the not-Open ticks already rebase the silence clock). Confirmed by
  `JetStreamContextTest::testWatchdogRecreateDeferredByReconnectRefiresOnceOpenAgain`, which
  fails pre-fix both at the latch (notified stuck true) and behaviorally (only 1 CONSUMER.CREATE
  ever — the watchdog never re-fires). The test sequences wire-event-driven (state flip inside the
  onDelete responder, deferral completion observed via `recreateInFlight`), not by wall clock.
  **Remediation after adversarial verification (3 lenses, no blockers):** added the sibling
  `testWatchdogRecreateDeferredAfterAdoptedInboxRefiresOnceOpenAgain` covering the
  `$newSid !== null` deferral arm (connection leaves Open inside the recreate's first CREATE, the
  adopted fresh inbox is released, and the second episode deletes the ROTATED candidate name);
  both tests kill the disabled-fix mutant. Two adjacent pre-existing nits found by the lenses were
  filed separately: [deferral-stale-candidate-adoption-suppresses-watchdog](deferral-stale-candidate-adoption-suppresses-watchdog.md)
  and [deferral-condition-sampled-after-reap-awaits](deferral-condition-sampled-after-reap-awaits.md).
- **Severity:** major
- **Type:** blocked flow (silent permanent stall) / regression of the round-1 fix contract
- **Area:** JetStream ordered consumers (and every KV/OS watch riding on them)
- **Where:** `src/JetStream/JetStreamContext.php:1533-1551` (deferral branch), `:3216`/`:3226`
  (watchdog latch gate/set), `src/JetStream/HeartbeatWatchdogState.php:79-83` (`touch()`)

## Problem

The new disconnect-collision deferral returns without clearing `$state->notified`:

```php
if ($this->client->state() !== ConnectionState::Open && !$state->stopped) {
    // ... unsubscribe($newSid) ...
    return; // comment promises: watchdog "fires again once Open"
}
```

But `armHeartbeatWatchdog`'s tick gates on the latch (`if ($state->notified || ...) return;`,
`:3216`) and sets `$state->notified = true` **before** invoking `onMiss` (`:3226`). The latch is
cleared only by an inbound frame's `touch()` or by the recreate **success** path (`:1494`). In the
watchdog-triggered (silent/reaped consumer) case no frame can ever arrive on the old inbox again —
the consumer was already dead (that is why `onMiss` fired) and the recreate's first step deleted it
anyway (`:1421`) — so after the deferral the watchdog ticks forever but early-returns on the latch.

The attempt budget (3 attempts, 50/100 ms delays, fail-fast requests while not Open) burns in
~150 ms, so any realistic reconnect window guarantees the deferral branch is taken.

## Failure scenario

A KV watch rides `subscribeOrderedConsumer`. The server restarts: the ephemeral watch consumer is
lost, the watchdog fires `onMiss` (latching `notified`), and the recreate's awaits collide with the
connection drop from the same restart. All attempts fail fast, the deferral branch returns. The
client reconnects and stays healthy — but no recreate ever runs again, no error is emitted (the
deferral intentionally skips `emitClientError`), and the deliver-inbox SUB, the `orderedStops`
entry, and the timer are all left alive. The watch silently misses every KV update forever.

Regression vs baseline: HEAD went terminal on this path but at least emitted
"Ordered consumer recreate failed" and tore the subscription down. The dispatch-gap-triggered case
is unaffected (`touch()` had cleared the latch when the gap frame arrived) — which is why no
existing test catches this.

## Suggested fix

Reset the silence episode in the deferral branch before returning: `$state->notified = false;`
(optionally also rebase `$state->lastActivityNs`, though the not-Open ticks already do). Then the
watchdog genuinely re-fires two idle intervals after the connection is Open again, as the comment,
the CHANGELOG, and `issues/kv-os-watch-no-gap-detection.md` all promise. Add a unit test:
watchdog-triggered recreate failing while state !== Open, then reconnect, then assert a second
`CONSUMER.CREATE` appears on the wire.
