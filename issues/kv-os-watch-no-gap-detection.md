# KV and Object Store watches have no sequence-gap detection or flow control — delivered updates can be silently lost mid-watch

- **Status:** FIXED (2026-08-07) — both watches are rebuilt on `subscribeOrderedConsumer` (extended
  with `consumerOverrides` for the watch deliver policies, an `onConsumerCreated` hook for the
  caught-up probe, and initial-policy recreates before first delivery so a `new`/`last_per_subject`
  watch never replays from sequence 1). Watches now get consumer-sequence gap detection with
  recreate-from-revision+1 (missed ranges REPLAYED), flow control, and watchdog-driven recreation
  of silent/reaped consumers. Confirmed by
  `KeyValueBucketTest::testWatchDetectsDeliveryGapAndRecreatesFromLastRevision` (fails pre-fix:
  out-of-order delivery, no recreate), the rewritten silent-consumer recreation tests (KV + OS),
  and the live integration test
  `testJetStreamKeyValueWatchWatchdogRecreatesReapedConsumerAndRecovers` (reap → recreate →
  post-reap updates delivered, against a real server).
  **Remediation after adversarial verification:** (a) a `stopped` latch on
  `HeartbeatWatchdogState` closes the stop-vs-in-flight-recreate race that could resurrect an
  unstoppable consumer (pinned by
  `testStopOrderedConsumerDuringInFlightRecreateDoesNotResurrect`, mutation-kill verified);
  (b) a recreate colliding with a connection drop now defers to the watchdog (which fires again
  once Open, fresh attempt budget) instead of dying terminally — the nats.go behavior;
  (c) a malformed stream-seq token can no longer reset the resume cursor to 0; (d) README and
  watch docblocks now direct stopping through `stopOrderedConsumer()`.
- **Severity:** major
- **Type:** message loss (silent gap)
- **Area:** KeyValue / ObjectStore watch
- **Where:**
  - `src/JetStream/KeyValue/KeyValueBucket.php:395-473` (`watch()` → `subscribeEphemeralPushConsumer`)
  - `src/JetStream/ObjectStore/ObjectStoreBucket.php:836-874` (same pattern)
  - Drop site: `src/Connection/NatsConnection.php:3160-3186` (`enqueueMessage` slow-consumer policy;
    defaults `maxPendingMessagesPerSubscription = 1024`, `SlowConsumerPolicy::DropOldest`,
    `src/Connection/NatsOptions.php:130-131`)

## Problem

Both watches use a plain ephemeral push consumer with `ack_policy: none`, no `flow_control`, and
no consumer-sequence tracking. The ordered-consumer machinery that provides exactly this
(`subscribeOrderedConsumer`, `JetStreamContext.php:1211+`: gap detection on consumer sequence +
recreate from `lastStreamSeq + 1`, flow control, heartbeat) exists in the codebase but is **not
used** for watches.

Consequences:

1. **Slow-consumer drops are permanent gaps.** A `last_per_subject`/`all` replay of a large
   bucket floods in while the user handler is slow; once >1024 frames queue on the watch sid the
   default DropOldest policy discards deliveries with only a debug-level emit. With
   `ack_policy: none` the server never redelivers. The watcher continues as if nothing was
   missed — for KV that means a stale key that is never observed again until it changes again.
2. **Reconnect gaps.** Frames in flight in the socket/parser at disconnect are lost; the SUB is
   replayed by `resubscribeAll()`, but the ephemeral consumer's cursor has already advanced past
   them server-side (or the consumer died with the server). No resume-from-revision happens.
3. **False caught-up signal.** `watch()`'s `numPending == 0` caught-up check
   (`KeyValueBucket.php:449-458`) can fire after such a gap, telling the caller the snapshot is
   complete when entries were dropped.

nats.go / nats.java KV and ObjectStore watchers are built on ordered consumers precisely so both
cases are lossless (recreate with `deliver_policy: by_start_sequence` from the last seen
revision).

## Suggested fix

Rebuild `watch()` on `subscribeOrderedConsumer` (KV watch delivers stream sequence = revision, so
the ordered `lastStreamSeq` maps directly to resume-from-revision), or at minimum:
track the consumer sequence from `$JS.ACK` metadata, and on a detected gap (or slow-consumer drop
on the watch sid) recreate the consumer from the last delivered revision + 1 instead of
continuing silently.
