# Pipelined ObjectStore uploads + JetStream 503 publish retry can permute chunk order — put() reports success for a corrupted object

- **Status:** FIXED (2026-08-07) — `put()`/`putStream()` now collect every chunk's acked stream
  sequence and verify strict monotonicity in chunk order (`assertUploadOrderPreserved()`); a
  permuted upload aborts BEFORE the meta publish, purges the partial NUID, and surfaces a clear
  retryable error instead of storing a corrupt object as success. Any upload failure now also
  purges the partial NUID (`abandonPartialUpload()`, closing `os-put-failure-orphans-chunks.md`
  as well). Confirmed by `ObjectStoreBucketTest::testPutAbortsAndPurgesWhenChunkAcksArriveReordered`
  and `testPutFailurePurgesPartialChunksBeforeRethrowing` (both fail pre-fix — the first stored the
  corrupt object successfully), plus 18 live ObjectStore integration tests.
  **Remediation after adversarial verification:** the first implementation collected acks via
  `Future\await()`, which returns results in COMPLETION order — vacuous for the real 503-retry
  shape (the retried chunk completes last with the highest sequence). Acks are now collected by
  sequential input-order awaits (`collectAckSequences()`), pinned by
  `testPutDetectsReorderEvenWhenAcksArriveInMonotonicCompletionOrder` (token-aware responder
  reproducing the real arrival pattern), and `abandonPartialUpload()` now drains in-flight
  publishes before purging so a chunk parked in the 503-retry delay cannot re-orphan itself
  post-purge.
- **Severity:** major
- **Type:** data corruption presented as success
- **Area:** ObjectStore put / JetStream publish retry
- **Where:**
  - `src/JetStream/ObjectStore/ObjectStoreBucket.php:264-289` (`put()` pipeline, `UPLOAD_PIPELINE_DEPTH = 16` in flight),
    `:337-345` (`putStream()` same pattern)
  - `src/JetStream/JetStreamContext.php:1729-1746` (`publishWithRetry`: any 503 is retried after
    `publishRetryWaitMs` ≈ 250 ms, default `publishRetryAttempts = 3`)

## Problem

The comment at `ObjectStoreBucket.php:268-271` argues stream order is preserved because "the PUB
frames are written to the single connection in chunk order". That holds for the *first* write of
each chunk — but each chunk goes through `JetStreamContext::publish()`, which **retries a 503
after a delay**. With up to 16 publishes in flight:

1. Chunk K's publish gets a 503 (brief leadership change / JetStream momentarily unavailable).
2. Chunks K+1…K+15, written moments later after the election resolves, are **accepted**.
3. K's retry fires 250 ms later and lands **after** them in the stream.
4. All acks succeed → `Future\await($pending)` is satisfied → the meta record is published with
   the digest computed over the *original* byte order → `put()` returns `ObjectInfo` success.
5. Every subsequent `get()`/`getToCallback()` reassembles in stream order and throws
   `Object digest mismatch` — the object is permanently corrupted, and the writer was told the
   write succeeded.

The digest check means the corruption is *detected* at read time, but the data is already
unrecoverable and the producer believed the upload succeeded.

## Suggested fix

Any of:
- Use a non-retrying publish for chunk uploads; on a 503, await all in-flight acks, then re-publish
  the failed chunk (and everything after it) so order is restored before continuing.
- Publish chunks with `Nats-Expected-Last-Subject-Sequence` chained per chunk subject so an
  out-of-order retry is rejected by the server instead of stored.
- On any chunk failure, abort the upload (purge the partial NUID — see
  `os-put-failure-orphans-chunks.md`) and surface the error instead of retrying inside the window.
