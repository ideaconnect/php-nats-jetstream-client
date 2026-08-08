# Deferral leaves the failed candidate's adoption state — a surviving old consumer can suppress the watchdog via touch()-before-name-filter until the next idle heartbeat

- **Status:** OPEN (filed 2026-08-08 during adversarial verification of the deferral-latch fix;
  nit — self-healing, no message loss)
- **Severity:** nit
- **Type:** delayed recovery in a narrow multi-failure edge
- **Area:** JetStream ordered consumers — disconnect-collision deferral
- **Where:** `src/JetStream/JetStreamContext.php:1455-1457` (adopt-before-await re-points
  `$consumerName`/`$deliver`/`$expectedConsumerSeq` at the candidate), deferral branch returns
  without restoring them; `:1619` (`touch()` runs before the consumer-name drop at `:1677-1684`)

## Problem

When the deferral is reached AFTER at least one create attempt ran, the adopt-before-await step
has already re-pointed the dispatch state at the never-created candidate, and the deferral does
not restore it. If — additionally — the episode's initial `deleteConsumer` failed to take effect
server-side (request timeout while still Open under a degraded JS API) and the old consumer
survived, its post-reconnect data frames call `$state->touch()` **before** being dropped by the
consumer-name filter: `lastActivityNs` keeps rebasing, so the watchdog can never observe two idle
intervals of silence while data flows — yet the handler delivers nothing (frames are
name-filtered).

Recovery is still guaranteed at the first delivery pause: an idle heartbeat reaches the tail-gap
check before any name scoping and triggers the next recreate, which resumes from
`lastStreamSeq+1` — no message loss, only delayed delivery. The property is pre-existing in the
deferral design (same round-1 batch), neither caused nor worsened by the latch fix, and cannot
arise in the watchdog-triggered scenario the major targeted (that consumer is dead — no frames).

## Suggested fix

Capture the pre-episode adoption state (old `$consumerName`, `$deliver`, `$expectedConsumerSeq`)
before the retry loop and restore it in the deferral branch alongside the latch reset, so a
surviving old consumer's frames pass the name filter and resume normal in-order
delivery/gap-checking immediately after reconnect instead of waiting for the first idle heartbeat.
