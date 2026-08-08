# addLink()/addBucketLink() silently overwrite an existing live object and orphan its chunks

- **Status:** FIXED (2026-08-08) — `addLink()`/`addBucketLink()` now refuse a name held by a LIVE
  object or link (nats.go ErrObjectAlreadyExists), and `addLink()` rejects deleted targets and
  link-to-link targets. Confirmed by `testAddLinkRejectsExistingLiveObject` and
  `testAddLinkRejectsDeletedTargetAndLinkTarget` (fail pre-fix).
  **Remediation after adversarial verification:** re-pointing an existing LINK is allowed (exact
  nats.go guard shape — only a live STORED object blocks; pinned by
  `testAddLinkAllowsRepointingAnExistingLink`), and the name-free lookup no longer swallows
  transient errors (a 503 during the check fails the addLink instead of reading as "free").
- **Severity:** minor
- **Type:** spec correctness / silent data loss (object becomes unreachable)
- **Area:** ObjectStore links
- **Where:** `src/JetStream/ObjectStore/ObjectStoreBucket.php:131-159`

## Problem

`addLink($name, ...)` publishes a rollup meta record for `$name` with no checks:

1. If a live object `$name` already exists (NUID N, chunks stored), the rollup **replaces its
   metadata with the link record in one call**: the object's content becomes unreachable, its
   chunks are never purged (no `lookupExisting()`/`purgeChunks()` on this path, unlike
   `put()`/`delete()`), and no error is raised.
2. nats.go `AddLink` rejects this (`ErrObjectAlreadyExists`), and additionally rejects links to
   *deleted* targets and links whose target is itself a link. None of these guards exist here.

## Suggested fix

Before publishing the link meta: `lookupExisting($name)` and fail with "object already exists"
when a non-deleted object (or link) is present; resolve the target and reject deleted targets
and link-to-link chains, mirroring nats.go. (If overwrite semantics are ever desired, they must
at least purge the replaced object's chunks.)
