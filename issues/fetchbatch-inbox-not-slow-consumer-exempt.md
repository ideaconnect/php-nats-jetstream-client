# fetchBatch() and directGetBatch() reply inboxes are not slow-consumer-exempt — a large burst in one read chunk silently drops replies

- **Status:** FIXED (2026-08-07) — both inboxes are now `markSubscriptionUnbounded()` immediately
  after subscribe, exactly like the pull-pipeline (#120) and mux (#118) inboxes; memory stays
  bounded by the requested batch. Confirmed by
  `JetStreamContextTest::testFetchBatchSurvivesSingleChunkBurstAboveThePendingCap` (1300 deliveries
  in one chunk, all returned; fails pre-fix at 1024) and
  `testDirectGetBatchSurvivesSingleChunkBurstAboveThePendingCap` (1100 replies + EOB, no truncation;
  fails pre-fix).
- **Severity:** major
- **Type:** message loss (silent, permanent for `max_deliver: 1`; truncated result presented as complete for direct get)
- **Area:** JetStream fetch / direct get
- **Where:**
  - `src/JetStream/JetStreamContext.php:2003-2023` (`_INBOX.JS.FETCH` subscribe — no `markSubscriptionUnbounded`)
  - `src/JetStream/JetStreamContext.php:795-836` (`_INBOX.JS.DGET` subscribe — same)
  - Contrast `:2215-2220`, where the pipelined pull inbox **is** marked unbounded for exactly this
    reason (#120), as is the mux request inbox (#118)
  - Drop mechanism: `src/Connection/NatsConnection.php:3160-3186` (`enqueueMessage` cap, default
    1024 per sid, default DropOldest; read chunk up to 131 072 bytes, `NatsOptions.php:143`)

## Problem

`NatsConnection::readIncoming()` enqueues **all** frames parsed from a chunk before draining.
A pull delivery frame for a tiny payload is ~75–90 wire bytes, so a single default 128 KiB read
chunk can carry ~1500 frames for the FETCH sid. Frames beyond the 1024 cap are discarded by the
slow-consumer policy (debug-level emit only) because the fetch inbox — unlike the pull-pipeline
and mux inboxes — was never exempted.

## Failure scenario

1. `fetchBatch(batch: 2000)` on a consumer with small messages; the server answers fast enough
   that >1024 delivery frames land in one read chunk.
2. Frames 1..~500 are DropOldest-discarded. The fetch returns a batch silently missing its head.
3. The server counted every one as delivered:
   - explicit-ack consumer → the dropped messages redeliver after `ack_wait` (skewed order,
     inflated `num_delivered`) — degraded but recoverable;
   - `max_deliver: 1` consumer → the dropped messages are **permanently lost**.
4. For `directGetBatch()` a dropped reply is never redelivered and the 204 end-of-batch marker
   still arrives → the call returns a truncated result presented as complete. (With default
   sizing DGET frames are large enough that this arm needs non-default
   `readChunkSizeBytes`/`maxPendingMessagesPerSubscription`, but the invariant should not depend
   on sizing.)

## Suggested fix

Call `$this->client->markSubscriptionUnbounded($sid)` immediately after both subscribes, exactly
as the pull-pipeline engine does. Memory stays bounded by the requested batch size, and the
subscription is removed in the existing `finally { unsubscribe }` blocks.
