# subscribeOrderedConsumer() returns a sid that goes stale after the first recreate — the ordered consumer can never be stopped afterwards

- **Status:** FIXED (2026-08-07) — together with `kv-os-watch-no-gap-detection.md`:
  `JetStreamContext::stopOrderedConsumer(int $sid)` resolves the CURRENT deliver sid through the
  consumer's shared state (registry keyed by the initial sid; deregistered on stop and terminal
  teardown), cancels the watchdog, unsubscribes the live inbox, and best-effort deletes the
  server-side ephemeral; unknown sids fall back to a plain unsubscribe. Confirmed by
  `JetStreamContextTest::testStopOrderedConsumerStopsAfterRecreateRotatedTheSid` (after a rotation
  from sid 2 to 3, stop-by-initial-sid unsubscribes sid 3 and deletes the current consumer).
- **Severity:** minor (no data loss, but an unstoppable subscription + timer)
- **Type:** API defect / resource leak
- **Area:** JetStream ordered consumer
- **Where:** `src/JetStream/JetStreamContext.php:1521` (returns the initial sid); inbox rotation
  at `:1279-1301` (new inbox + sid), `:1343` (state adopts new sid), `:1351-1356` (old sid
  unsubscribed)

## Problem

Every recreate (sequence gap, tail-gap heartbeat, watchdog miss, terminal 4xx status — routine
events under load) rotates the deliver inbox to a *new* sid and unsubscribes the old one. The
caller only ever holds the `int` returned by `subscribeOrderedConsumer()`. After one rotation:

- `unsubscribe($returnedSid)` matches nothing (`NatsConnection::unsubscribe` returns silently
  for an unknown sid), and there is no other public handle;
- the replacement subscription, its ephemeral consumer, and the re-armed heartbeat watchdog keep
  delivering/recreating forever until the whole connection is torn down — the application has no
  way to stop or detach the handler.

nats.go keeps a stable `Subscription`/`ConsumeContext` handle across recreates.

## Suggested fix

Return a stable handle object (or register an alias) that tracks the current
`$state->deliverSid`: e.g. return a small `OrderedConsumerHandle` with a `stop()` that
unsubscribes the *current* sid, cancels the watchdog, and best-effort-deletes the ephemeral
consumer. If the `int` return type must be kept for BC, add a
`stopOrderedConsumer(int $initialSid)` that resolves the live sid through the shared state.
