# addLink tombstone test pins the full exception message with assertSame — over-pinning wording the next reword legitimately changes

- **Status:** FIXED (2026-08-08) — the tombstone test now pins the contract (the object name plus `already
  exists`) with assertStringContainsString instead of the full message via assertSame, matching
  the sibling live-object test.
- **Severity:** nit
- **Type:** test quality (over-pinning)
- **Area:** tests — ObjectStore links (2nd-round fix's pinning test)
- **Where:** `tests/Unit/ObjectStoreBucketTest.php:940` (`testAddLinkRejectsDeletedTombstoneName`)

## Problem

The test asserts the complete exception message with `assertSame`. The contract is "the name is
blocked with the object-already-exists error" — the exact phrasing is not; a future message
reword (e.g. adding the bucket name) breaks the test without any behavior change.

## Suggested fix

Pin the discriminating substring (`already exists`) plus the object name via
`assertStringContainsString`, matching how the sibling live-object test pins its message.
