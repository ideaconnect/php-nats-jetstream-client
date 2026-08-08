# assertUploadOrderPreserved() returns (not continues) on a non-positive sequence — one unreported seq disables order verification for all remaining chunks

- **Status:** FIXED (2026-08-08) — the non-positive-sequence arm now `continue`s (leaving $previous
  untouched), so later verifiable pairs are still checked. Pinned by
  `testPutOrderCheckStillDetectsInversionAfterSeqlessAck` (pre-fix red: [1,0,5,3] stored the
  corrupted object as success; now aborts and purges).
- **Severity:** nit
- **Type:** defensive-check completeness
- **Area:** ObjectStore upload ordering (round-1 fix follow-up)
- **Where:** `src/JetStream/ObjectStore/ObjectStoreBucket.php:1246-1250`

## Problem

```php
foreach ($ackSequences as $sequence) {
    if ($sequence <= 0) {
        // The server did not report a sequence (defensive): order cannot be verified.
        return;
    }
    ...
}
```

A seq of 0 can only arise from a success-shaped PubAck missing `seq` (`PubAck::fromArray`
defaults to 0; `parsePublishAck` validates `stream` but not `seq`), which no real server
produces — but when it does occur at position i, inversions among the valid sequences at positions
i+1..n also go unchecked, silently reverting those chunks to the pre-fix corrupt-as-success
behavior. Practically unreachable today, hence nit.

## Failure scenario

Hypothetical intermediary/future server returns one ack without `seq` at chunk 3 of 40; chunk
20's 503-retried publish lands after chunk 21. The check returns at chunk 3, the inversion at
20/21 is never seen, the meta record publishes, and put() reports success for a corrupted object —
exactly the case the round-1 fix targets.

## Suggested fix

Use `continue;` (leaving `$previous` untouched) so verifiable neighbor pairs after an unreported
sequence are still checked.
