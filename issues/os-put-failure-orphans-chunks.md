# Failed ObjectStore upload leaves orphaned chunks — no partial-purge cleanup (nats.go purgePartial parity missing)

- **Status:** FIXED (2026-08-07) — together with `os-put-503-retry-chunk-reorder.md`:
  `abandonPartialUpload()` best-effort purges the fresh NUID's chunks on every failed/aborted
  upload path (chunk failure, order violation, meta-publish failure, producer throw) before the
  original error is rethrown. Confirmed by
  `ObjectStoreBucketTest::testPutFailurePurgesPartialChunksBeforeRethrowing` (fails pre-fix).
- **Severity:** minor
- **Type:** completeness / resource leak (unbounded stream growth)
- **Area:** ObjectStore put
- **Where:** `src/JetStream/ObjectStore/ObjectStoreBucket.php:246-307` (`put()`), `:320-394` (`putStream()`)

## Problem

When chunk N fails after chunks 1…N-1 were acked (or the `putStream()` producer throws, or
`publishMeta()` fails), the exception is surfaced — so there is no silent success — but the
already-stored chunks under the fresh NUID are never purged. No meta record references that
NUID, so no later `put()`/`delete()` will ever purge them (the purge paths key off the meta's
NUID). The orphaned chunks accumulate in the stream until the bucket is deleted.

nats.go purges the partial chunk subject (`purgePartial`) on any Put error, per ADR-20 cleanup
behavior.

## Suggested fix

Wrap the chunk-publish + meta-publish phase in try/catch; on failure, best-effort
`purgeChunks($nuid)` (the helper already exists for the previous-revision cleanup) before
rethrowing. In `putStream()` the same cleanup applies when the producer callback throws.
