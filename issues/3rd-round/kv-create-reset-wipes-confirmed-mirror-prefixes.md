# KV create() resets CONFIRMED mirror prefixes before server confirmation — a failed re-create silently blinds a working mirror handle

- **Status:** FIXED (2026-08-08) — the readPrefix/writePrefix reset moved to AFTER the successful createStream
  await, atomic with the re-apply (mirroring bind()'s placement): a failed create can no longer
  wipe server-confirmed prefixes, and no suspension window separates clear from re-apply. Pinned
  by `KeyValueBucketTest::testFailedRecreateKeepsConfirmedMirrorPrefixes` (confirmed cross-domain
  mirror, then a rejected second create: writes still route via `$JS.HUB.API.$KV.src.` and reads
  via `$KV.src.`), verified red with the reset moved back above the await; the two round-2 tests
  stay green.
- **Severity:** minor
- **Type:** silent read blindness / write misdirection on an edge path (introduced by the
  2nd-round fix's reset placement)
- **Area:** KV mirror support (2nd-round fix follow-up)
- **Where:** `src/JetStream/KeyValue/KeyValueBucket.php:102-103` (reset at the TOP of create()'s
  closure), `:125` (kvMirrorConfig can throw client-side), `:138-142` (createStream await),
  `:149` (re-apply only on success); contrast `bind()` `:343-344` (resets only AFTER getStream()
  succeeded, atomically with the re-apply)

## Problem

The 2nd-round fix's reset runs at the top of `create()`'s async closure — before the
config-mapping code that can throw client-side and before the `createStream()` await. On a handle
that previously fronted a server-CONFIRMED mirror (earlier successful `create()` or `bind()`),
any failed `create()` attempt clears the confirmed prefixes and the re-apply never runs. That
violates the fix's own principle (the cleared prefixes WERE confirmed), deviates from the cited
nats.go parity (a failed CreateKeyValue never mutates a working handle), and is inconsistent with
`bind()` in the same diff. The pre-await placement also opens a suspension window during a
SUCCESSFUL re-create in which concurrent fibers on the handle read/write with plain subjects.

## Failure scenario

`bind()` to an existing cross-domain mirror works (reads via `$KV.src.`, writes via
`$JS.HUB.API.$KV.src.`). A startup/reconnect ensure path re-runs
`create(['mirror' => [...]])` and it fails (timeout, err 10058, or the client-side
domain+external throw before any request). The prefixes are already cleared: every subsequent
`get()` 404s against the wrong subjects (null for every key), `watch()` observes nothing, and
`put()` publishes to a subject no stream ingests (503) — until a later successful create/bind.

## Suggested fix

Move the reset to immediately after the successful await, atomic with the re-apply (mirroring
bind()):
`$info = ...->await(); $this->readPrefix = null; $this->writePrefix = null; if ($mirror !== null) { $this->applyMirrorPrefixes($mirror); }`.
Both existing pinning tests keep passing (a fresh handle's prefixes were already null); add a
test that binds to a mirror, fails a subsequent create(), and asserts puts still target the
origin prefix.
