# ObjectStore watch() pattern matches against base64url-encoded name tokens — any non-'>' pattern silently observes nothing

- **Status:** FIXED (2026-08-08) — `watch()` base64url-encodes an exact-name pattern into the
  filter subject and REJECTS wildcard patterns with a clear error (they can never match encoded
  tokens; previously a silent no-op watch). Confirmed by
  `testWatchEncodesExactNamePatternAndRejectsWildcards` (fails pre-fix).
- **Severity:** minor
- **Type:** API correctness / silent no-op
- **Area:** ObjectStore watch
- **Where:** `src/JetStream/ObjectStore/ObjectStoreBucket.php:836-839` (filter =
  `metaPrefix() . $pattern`) together with `:1008-1019` (`metaSubject()` base64url-encodes object
  names per ADR-20)

## Problem

Meta subjects are `$O.<bucket>.M.<base64url(name)>`. `watch($handler, $pattern)` appends the
caller's pattern verbatim to the meta prefix, so a human-readable pattern such as
`watch($h, 'logs-*')` produces the filter `$O.b.M.logs-*`, which can never match an encoded
token. The subscription is created successfully and simply never delivers — indistinguishable
from "no changes", rather than an error. Nothing documents that only `'>'` (the default) is
meaningful.

## Suggested fix

Either restrict the parameter (accept only `'>'`, throw on anything else with a pointer to
client-side filtering), or translate patterns: an exact object name should be encoded via the
same base64url helper before appending; wildcard patterns cannot be mapped onto encoded tokens
and should be rejected with a clear error.
