# KV getAll()/keys() and ObjectStore list() have no fallback for buckets without Direct Get, unlike get()/info()

- **Status:** FIXED (2026-08-08) — the KV `getAll()` and OS `list()` per-subject lookups now catch
  the Direct Get 503 and fall back to the leader STREAM.MSG.GET path exactly like `get()`/`info()`.
  Confirmed by `testGetAllFallsBackToStreamMessageWhenDirectGetUnavailable` and
  `testListFallsBackToStreamMessageWhenDirectGetUnavailable` (fail pre-fix).
  **Remediation after adversarial verification:** the BATCHED path (servers 2.11+, where the
  per-subject fan-out never ran) now catches the multi_last 503 and falls through to the
  fan-out + leader read — pinned by `testGetAllFallsBackWhenBatchedDirectGetAnswers503`.
- **Severity:** minor
- **Type:** completeness / interop gap (loud failure, but a working reference-client operation fails)
- **Area:** KeyValue / ObjectStore enumeration
- **Where:** `src/JetStream/KeyValue/KeyValueBucket.php:782-793` (catches 404 only),
  `src/JetStream/ObjectStore/ObjectStoreBucket.php:900-921` (same pattern); contrast the 503
  fallbacks in single-record reads (`KeyValueBucket.php:261-265`, `ObjectStoreBucket.php:702-706`)

## Problem

On a bucket whose stream lacks `allow_direct` (legacy buckets predating `allow_direct`, or a
caller override at create time), the Direct Get API answers 503. `get()`/`info()` catch that and
fall back to the leader `STREAM.MSG.GET` API — but the per-subject lookups inside `getAll()` and
`list()` catch **only 404**, so the 503 propagates and the whole enumeration fails on a bucket
whose single-key reads work fine. The batched (ADR-31) fast path is properly gated on server
support, but the per-subject fan-out is not gated on *stream* support.

## Suggested fix

In both enumeration lambdas, catch the 503 `JetStreamException` and fall back to the same
leader-read helper `get()`/`info()` use (or detect `allow_direct` once from the STREAM.INFO the
enumeration already fetched and choose the path up front).
