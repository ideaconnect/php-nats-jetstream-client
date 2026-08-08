# Header-name lookup is exact-case only — keys from non-canonicalizing publishers miss lookups that Go clients would match

- **Status:** FIXED (2026-08-08) — new `NatsHeaders::get()` case-insensitive accessor (exact-case
  hit wins) bridges the interop asymmetry with canonicalizing readers; decode stays verbatim.
  Confirmed by `testGetLooksUpHeaderNamesCaseInsensitively`.
- **Severity:** minor (low confidence as a spec matter; reported as an interop asymmetry)
- **Type:** interop asymmetry
- **Area:** Core headers
- **Where:** `src/Core/NatsHeaders.php` — `fromWireBlock()` / `fromWireBlockMulti()` return keys
  verbatim; all lookups are exact-case array access

## Problem

A publisher emitting `nats-msg-id:x` (lowercase) is delivered as-is. nats.go canonicalizes
header names on read (textproto MIME canonicalization), so a Go consumer sees `Nats-Msg-Id`;
PHP user code reading `$headers['Nats-Msg-Id']` misses the lowercase key entirely. The client's
own system-header reads (`Status`, JetStream `Nats-*` headers) target server-generated canonical
lines and are unaffected; `Service::buildObserverContext()` already lowercases defensively.

Wire header names are case-sensitive per the NATS docs and ecosystem behavior is mixed, so this
is not a clear-cut spec violation — but it is an asymmetry with the reference client that shows
up when consuming messages produced by non-canonicalizing publishers.

## Suggested fix

Either canonicalize names on decode (Go parity), or add a case-insensitive accessor
(`NatsHeaders::get($headers, $name)`) and use it for user-facing docs/examples; document the
chosen behavior explicitly.
