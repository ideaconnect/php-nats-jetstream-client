# $SRV discovery responses serialize empty `metadata` as JSON `[]` instead of `{}` — breaks Go-based consumers (nats CLI, nats.go micro)

- **Status:** FIXED (2026-08-07) — discovery payloads are now encoded via
  `Service::encodeDiscoveryPayload()`, which forces empty `metadata` maps (service-level and
  per-endpoint) to `{}` at the wire boundary while `statsSnapshot()` keeps returning plain arrays
  to PHP callers. Confirmed by `ServiceTest::testDiscoveryEncodesEmptyMetadataAsObject`
  (PING/INFO/STATS all carry `"metadata":{}`; fails pre-fix on `"metadata":[]`).
- **Severity:** major
- **Type:** spec correctness / interop failure
- **Area:** Services (NATS micro, ADR-32)
- **Where:** `src/Services/Service.php:751-756` (`$base` used by PING/INFO/SCHEMA), `:569`
  (`statsSnapshot()`), `:788` (per-endpoint `metadata` in `$SRV.INFO`)

## Problem

`metadata` is a plain PHP array defaulting to `[]`. `json_encode([])` emits `[]` (a JSON array),
but ADR-32 defines `metadata` as `map[string]string`. Go's `json.Unmarshal` of `[]` into
`map[string]string` hard-fails (`cannot unmarshal array into Go struct field … of type
map[string]string`), so the **entire discovery response is rejected**: the service is invisible
or errors in `nats micro ls` / nats.go micro clients whenever metadata is empty — the default.

Per-endpoint `metadata` in `$SRV.INFO` has the same problem even when service-level metadata is
set. The repo already recognized and fixed this exact interop class for ObjectStore (#109);
Services was not covered (`tests/Unit/ServiceTest.php` only asserts the non-empty case).

## Suggested fix

Emit an object for empty maps, e.g. cast to `\stdClass` when empty
(`$this->metadata === [] ? new \stdClass() : $this->metadata`) or encode the payload with a
helper that converts empty `metadata` fields, in all four sites (PING/INFO/SCHEMA base, stats
snapshot, per-endpoint INFO entries). Add unit tests asserting the wire JSON contains
`"metadata":{}`.
