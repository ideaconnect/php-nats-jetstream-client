# create() applies mirror read/write prefixes BEFORE createStream() succeeds, and they are never reset — a failed mirror create permanently poisons the handle

- **Status:** FIXED (2026-08-08) — create() awaits createStream() first and applies mirror prefixes only on
  success; create() and bind() reset readPrefix/writePrefix before conditionally re-applying.
  Pinned by `testFailedMirrorCreateLeavesHandleOnPlainBucketSubjects` and
  `testRebindToNonMirrorStreamClearsStalePrefixes` (both pre-fix red on the misdirected wire
  subjects).
- **Severity:** minor
- **Type:** silent write misdirection / read blindness (lost messages from the caller's view)
- **Area:** KV mirror support (round-1 ADR-57 fix follow-up)
- **Where:** `src/JetStream/KeyValue/KeyValueBucket.php:114-124` (prefixes applied pre-await),
  `:132-136` (createStream awaited after), `:282-287` (`applyMirrorPrefixes` — only assignment
  site, never nulled), `:315-317` (`bind()` sets but never clears)

## Problem

Inside `create()`'s async closure, `applyMirrorPrefixes($mirror)` runs **before**
`createStream()` is awaited. `$readPrefix`/`$writePrefix` are assigned only in
`applyMirrorPrefixes()` and are never nulled anywhere — `bind()` likewise only *sets* them when
STREAM.INFO shows a mirror, never clears them. So:

- A **failed** mirror create (origin unreachable; err 10058 name-in-use-with-different-config;
  timeout) leaves the handle with mirror prefixes for a configuration that never existed
  server-side. Every subsequent write routes to the origin prefix (cross-domain: through the
  foreign `$JS.<domain>.API`), and cross-domain reads miss every local record.
- A handle that previously fronted a mirror keeps stale prefixes after the bucket is re-created or
  re-bound as a non-mirror bucket.

nats.go only builds the KV handle (`mapStreamToKVS`) from a **successful** create, so a failed
`CreateKeyValue` can never poison a usable handle.

## Failure scenario

```php
$kv = $js->keyValue('dst');
try { $kv->create(['mirror' => ['bucket' => 'src', 'domain' => 'HUB']])->await(); }
catch (JetStreamException) { /* origin unreachable, or KV_dst exists with different config */ }
$kv->create()->await(); // falls back to a plain bucket — succeeds
$kv->put('k', 'v');     // publishes to $JS.HUB.API..$KV.src.k — the WRONG bucket/domain
$kv->get('k');          // direct-gets $KV.src.k against KV_dst → null for every key stored
```

Silent write misdirection and read blindness on a bucket whose create "failed".

## Suggested fix

Await `createStream()` first and call `applyMirrorPrefixes()` only after it succeeds.
Additionally reset `$this->readPrefix = $this->writePrefix = null;` at the top of `create()` and
`bind()` (before conditionally re-applying), so a handle can never carry prefixes from a
configuration that was not confirmed server-side.
