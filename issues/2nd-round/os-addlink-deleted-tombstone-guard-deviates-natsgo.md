# addLink() allows linking over a DELETED object's tombstone — nats.go rejects it with ErrObjectAlreadyExists, contradicting the "exact nats.go guard shape" claim

- **Status:** OPEN (filed 2026-08-08, second-round review; adversarially verified against
  nats.go v1.52.0 source, both legacy and jetstream APIs)
- **Severity:** minor
- **Type:** cross-client behavioral divergence / incorrect parity claim in docs
- **Area:** ObjectStore links (round-1 fix follow-up)
- **Where:** `src/JetStream/ObjectStore/ObjectStoreBucket.php:188` (guard), `:174-180` (docblock
  claiming nats.go parity), `issues/os-addlink-overwrites-live-object.md` (Status block repeating
  the claim)

## Problem

The PHP guard is:

```php
if ($existing !== null && !$existing->deleted && !$existing->isLink()) { throw ...; }
```

— a deleted tombstone (`deleted === true`) passes. nats.go v1.52.0 (`object.go:778-785`,
identical in `jetstream/object.go:1023-1030`) does
`GetInfo(name, GetObjectInfoShowDeleted())` — which returns tombstones — and returns
`ErrObjectAlreadyExists` whenever the info is non-nil and `!isLink()`. A deleted regular object's
tombstone is not a link (nats.go `Delete` keeps the meta, only sets `Deleted`/zeroes size), so
nats.go **rejects** `put → delete → addLink` on the same name while this client allows it. The
docblock ("An existing LINK is deliberately allowed — nats.go permits re-pointing an alias") and
the round-1 issue Status ("exact nats.go guard shape — only a live STORED object blocks") both
claim parity that does not hold for the tombstone case.

(Related second-round check: the live-LINK-overwrite allowance itself was adversarially reviewed
and **is** correct nats.go parity — only the tombstone case diverges.)

## Failure scenario

`put('foo', ...)` then `delete('foo')`, then `addLink('foo', 'target')`: this client creates the
link and returns ObjectInfo; nats.go against the same bucket fails with `ErrObjectAlreadyExists`.
Cross-client behavior diverges on shared buckets, and a maintainer trusting the parity comment
will not expect it. No data loss (the tombstone has no chunks) — hence minor.

## Suggested fix

Either match nats.go exactly — block whenever a non-link record exists, deleted or not
(`if ($existing !== null && !$existing->isLink())`) — or keep the deliberate relaxation but
correct the docblock and the round-1 issue Status block so they no longer claim exact nats.go
guard shape.
