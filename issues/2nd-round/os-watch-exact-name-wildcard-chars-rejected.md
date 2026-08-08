# ObjectStore watch() rejects exact names containing '*'/'>' even though such names are legal, stored base64url-encoded, and would filter correctly

- **Status:** FIXED (2026-08-08) — watch() gained `bool $exactName = false`: when true the pattern is always
  base64url-encoded (names containing '*' or '>' work); the default rejection message now names
  the escape hatch. Pinned by `testWatchExactNameEncodesNameContainingWildcardChars` (pre-fix
  red).
- **Severity:** minor
- **Type:** self-contradictory API restriction
- **Area:** ObjectStore watch (round-1 fix follow-up)
- **Where:** `src/JetStream/ObjectStore/ObjectStoreBucket.php:928-937` (guard inspects the RAW
  pattern before encoding), `:1482-1487` (`assertValidName` accepts any non-empty name)

## Problem

The new wildcard guard runs on the raw pattern **before** base64url encoding:

```php
if ($pattern !== '>') {
    if (str_contains($pattern, '*') || str_contains($pattern, '>')) {
        throw new JetStreamException('... Use ">" for all objects or an exact object name ...');
    }
    $pattern = $this->encodeName($pattern);
}
```

But `assertValidName()` deliberately accepts ANY non-empty name ("Names are base64url-encoded into
the meta subject, so any non-empty name is acceptable"), so `put('a*b')` / `put('report>2024')`
succeed and their meta subjects are safe base64url tokens. `encodeName()` would produce a
perfectly valid exact filter for these names — yet the guard throws first, and the exception's own
message instructs the caller to pass "an exact object name", which is precisely what they did.

## Failure scenario

`put('report>2024', $data)` succeeds; `watch($handler, 'report>2024')` throws
"Object Store watch patterns cannot use subject wildcards". The only workaround is `watch('>')`
plus client-side filtering.

## Suggested fix

Since names are always encoded, the wildcard ambiguity only exists for the literal `'>'` pattern:
either add an explicit escape (a `bool $exactName` parameter, or a dedicated `watchObject($name)`
that always encodes), or document in the exception message and watch() docblock that names
containing `*`/`>` must use `'>'` + client-side filtering.
