# Service endpoint names are not validated against ADR-32 naming rules (service name/version are)

- **Status:** FIXED (2026-08-08) — `addEndpoint()` enforces the ADR-32 token rules
  (`^[A-Za-z0-9_-]+$`) exactly like the service-name constructor check. Confirmed by
  `testAddEndpointRejectsNonTokenName` (fails pre-fix).
- **Severity:** minor
- **Type:** spec correctness (ADR-32)
- **Area:** Services (NATS micro)
- **Where:** `src/Services/Service.php:98-109` (`addEndpoint()` — only a non-empty trim check)

## Problem

ADR-32 requires endpoint names to satisfy the same token rules as service names
(`^[A-Za-z0-9_-]+$`); nats.go micro rejects invalid names at registration. This client accepts
`addEndpoint('my endpoint!', ...)` and then advertises the invalid name verbatim in
`$SRV.INFO`/`$SRV.STATS` payloads consumed by conformant tooling, which may reject or misrender
them. The constructor already enforces the rule for the service name — the same check is simply
missing for endpoints.

## Suggested fix

Apply the existing service-name validation regex to `$name` in `addEndpoint()` (and to group
names in `ServiceGroup` if not already covered), throwing `InvalidArgumentException` on
violation.
