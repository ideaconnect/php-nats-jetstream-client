# directGetLastForSubjects(): a chunk with zero matches throws 404 and discards every other chunk's already-fetched results

- **Status:** FIXED (2026-08-08) — `directGetLastForSubjects()` treats a chunk's lone 404 as
  "no matches for these subjects" (ADR-31), contributing zero messages instead of discarding every
  other chunk's results; an all-absent set returns `[]`. Confirmed by
  `testDirectGetLastForSubjectsTreatsAllMiss404AsEmpty` (fails pre-fix).
- **Severity:** minor
- **Type:** spec correctness (ADR-31) / spurious failure
- **Area:** JetStream batched Direct Get
- **Where:** `src/JetStream/JetStreamContext.php:700-714` (sequential chunk loop), `:889-894`
  (`directGetBatch()` throws on any recorded >= 400 status; ADR-31 servers answer an all-miss
  `multi_last` with a lone 404)

## Problem

The chunking docblock (`:668-674`) claims "concatenating the per-chunk replies yields the same
result a single (permitted) request would". That is false whenever one chunk matches nothing:

1. 1500 exact subjects → two chunks (1000 + 500). All stored messages fall in chunk 1; chunk 2's
   subjects have no stored messages (deleted/purged/never written).
2. The server answers chunk 2 with a lone 404 status → `directGetBatch()` throws
   `JetStreamException(404)` → the whole call aborts, discarding chunk 1's 1000 valid messages.
3. A single un-chunked request would simply have returned those 1000 messages with the absent
   subjects missing.

Same root cause: an all-absent single-chunk call throws 404 instead of returning `[]`, so the KV
`getAllBatched()` / ObjectStore `listBatched()` fast paths can throw spuriously when keys are
purged between STREAM.INFO and the batch — where the per-subject fan-out path treats 404 as
"skip".

## Suggested fix

In `directGetLastForSubjects()` (or in `directGetBatch()` when the request is a `multi_last`),
treat a 404 terminal status as "no matches for this chunk" — contribute zero messages and
continue — rather than an error. Real errors (400s other than 404, 5xx) keep throwing.
