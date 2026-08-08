# list()'s 503 fallback round-trips meta-subject token → name → re-encoded subject — lossy for non-canonical (unpadded) tokens, silently dropping those records

- **Status:** FIXED (2026-08-08) — the fallback queries the leader by the ENUMERATED subject via the new
  fetchInfoBySubject() helper (fetchInfo() delegates to it, semantics preserved). Pinned by
  `testListFallbackQueriesLeaderByEnumeratedSubjectForNonCanonicalToken` (pre-fix red:
  last_by_subj carried the re-padded QQ== instead of the enumerated QQ).
- **Severity:** nit
- **Type:** interop edge (foreign-client-written buckets) / path inconsistency
- **Area:** ObjectStore enumeration fallback (round-1 fix follow-up)
- **Where:** `src/JetStream/ObjectStore/ObjectStoreBucket.php:1038-1041` (decode + fetchInfo),
  `:1465-1468` (`base64Url()` re-encodes WITH padding)

## Problem

The per-subject 503 fallback decodes the meta-subject token to a name and lets `fetchInfo()`
re-encode it:

```php
$encoded = substr($subject, strlen($this->metaPrefix()));
$name = base64_decode(strtr($encoded, '-_', '+/'), true);
return is_string($name) ? $this->fetchInfo($name, false) : null;
```

`fetchInfo()` rebuilds the subject via `base64Url()` **with padding**. An unpadded token like `QQ`
decodes to `A` but re-encodes to `QQ==` — a different subject — so the leader read 404s and the
record is silently dropped; a non-base64 token returns null directly. The Direct Get paths decode
only the message BODY and would return the record for the same subject. All reference clients
(nats.go/java/py) pad, so this is interop-edge only — hence nit.

## Failure scenario

A bucket written by a non-canonical client (unpadded name tokens) is enumerated: on an
allow_direct-enabled bucket list() returns the object; on an allow_direct-disabled bucket the 503
fallback silently omits it — same call, different results depending only on which path ran.

## Suggested fix

Query the leader by the subject the enumeration already has instead of round-tripping through the
name: call `requestStreamMessage($subject)` (plus the decodeMetadataFromApiMessage/seq hydration
fetchInfo does) so the fallback reads exactly the subject the stream reported.
