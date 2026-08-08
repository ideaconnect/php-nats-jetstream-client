# orderedStops registry leaks on plain unsubscribe($sid) — the documented legacy stop path strands one closure + watchdog state per watch, forever

- **Status:** FIXED (2026-08-08) — HeartbeatWatchdogState gained an onDefunct closure (WeakReference-based);
  the watchdog's self-cancel tick invokes it only when the firing timer is the CURRENT one,
  releasing the orderedStops entry and best-effort deleting the ephemeral. Pinned by
  `testPlainUnsubscribeReleasesOrderedStopRegistryAndDeletesConsumer` (pre-fix red: entry leaked)
  and `testRotatedOutWatchdogTimerDoesNotReleaseStopRegistryOrDeleteConsumer` (red against a
  removed current-timer guard).
- **Severity:** minor
- **Type:** unbounded memory growth / missed server-side consumer cleanup
- **Area:** JetStream ordered consumers / KV+OS watches (round-1 fix follow-up)
- **Where:** `src/JetStream/JetStreamContext.php:1701` (registry entry), `:1589` and `:1760`
  (the only two removal sites), `:3198-3206` (watchdog self-cancel branch — WeakReference-only,
  structurally cannot clean the registry), `src/JetStream/KeyValue/KeyValueBucket.php:582-585`
  (docblock sanctioning plain unsubscribe pre-recreate)

## Problem

Every ordered consumer (so every KV/OS watch) registers
`$this->orderedStops[$sid] = function () use ($state, $stream, &$consumerName) {...}`. The only
removals are terminal recreate failure and `stopOrderedConsumer()`. `NatsConnection::unsubscribe`
knows nothing of the registry, and the watchdog's self-cancel branch that detects the dead sid is
a static closure holding only WeakReferences — it has no JetStreamContext reference and cannot
unset the entry. Baseline HEAD's watch() required exactly `unsubscribe($sid)` as the stop
mechanism and left no residue; the new watch() docblock still sanctions that path ("a plain
unsubscribe($sid) only works until the first recreate").

Each stranded entry roots the stop closure, the `HeartbeatWatchdogState`, the user handler, and
the consumer-option arrays; sids are monotonic, so entries never collide or get overwritten. The
ephemeral consumer is also left to server-side interest-loss reaping instead of the proactive
delete `stopOrderedConsumer()` performs.

## Failure scenario

A long-running app upgraded from the released version keeps its existing pattern — one short-lived
watch per request: `$sid = $kv->watch(...)->await(); ...; $client->unsubscribe($sid)->await();`.
Delivery does stop (the watchdog tick sees the inactive sid and self-cancels), but each cycle
strands one `orderedStops` entry, so memory grows without bound over the process lifetime.

## Suggested fix

Have the connection's unsubscribe path notify JetStreamContext, or give the watchdog's
self-cancel branch a way to release the entry (e.g. register a cleanup callback alongside the
stop closure that unsets `orderedStops[$initialSid]` and runs the best-effort consumer delete),
so the legacy stop path releases the registry entry.
