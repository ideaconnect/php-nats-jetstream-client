# Code review 2026-08-07 — findings index

Full review of the client against the NATS.io specification (client protocol, JetStream ADRs,
KV ADR-8, ObjectStore ADR-20, micro ADR-32, RFC 6455/7692 for WebSocket), focused on lost
messages and blocked flow. Every finding below was verified against the current code at the
cited `file:line` locations. 27 issues: 1 critical, 10 major, 16 minor.

**Status update (2026-08-07, same day):** the critical issue, ALL 10 major issues, and 3 minors
(`os-put-failure-orphans-chunks`, `ordered-consumer-stale-sid-handle` — absorbed by the major
fixes — and the WS test-double hardening) are **FIXED**, each confirmed by tests that fail against
the pre-fix code, plus live-server integration where applicable. See each file's `Status:` block
for the fix shape and confirming tests. A 4-lens adversarial verification workflow over the batch
then found (and led to fixing, each with its own mutation-verified test) two genuine defects in the
first round of fixes — a completion-order flaw that made the upload-order guard vacuous, and a
stop-vs-recreate race — plus several hardening items; the affected Status blocks carry
"Remediation after adversarial verification" notes. Gates at closure: 1776 unit tests, PHPStan
level 8 clean, 138 live integration tests, 47 Behat scenarios.

**Status update (2026-08-08):** the remaining 14 minors are now **FIXED** as well — every issue in
this index is closed, each with tests confirmed to fail against the pre-fix code (see the
`Status:` block in each file). The repository-wide sweep is complete: 27/27 findings resolved.

Areas checked and found sound (no issue filed): `ProtocolParser` framing (probed across chunk
splits, CRLF payloads, HMSG accounting), `ProtocolCodec` wire grammar and CONNECT fields,
`NatsConnection` reconnect/replay/pong-correlation/drain machinery, `AmpSocketTransport`,
`SubscriptionQueue` bounds, pull-request JSON and ack grammar (ADR-13/ADR-9), KV
digest/DEL-PURGE/CAS semantics, BatchPublisher ack verification, `$SRV` verb coverage.

## Critical

| Issue | Area | Type |
|---|---|---|
| [ws-close-write-wedge](ws-close-write-wedge.md) | WS transport | Deadlock: close() can suspend forever, wedging recovery permanently |

## Major

| Issue | Area | Type |
|---|---|---|
| [ws-unguarded-pong-write-drops-frames](ws-unguarded-pong-write-drops-frames.md) | WS transport | Message loss on ping-reply write failure |
| [transport-write-no-timeout-drain-flush-unbounded](transport-write-no-timeout-drain-flush-unbounded.md) | Transport/Connection | drain()/flush() can hang forever under write backpressure |
| [kv-os-watch-no-gap-detection](kv-os-watch-no-gap-detection.md) | KV/ObjectStore | Watches silently lose updates (no ordered-consumer machinery) |
| [os-watch-no-idle-heartbeat](os-watch-no-idle-heartbeat.md) | ObjectStore | Dead watch hangs forever (no watchdog, unlike KV) |
| [os-put-503-retry-chunk-reorder](os-put-503-retry-chunk-reorder.md) | ObjectStore | Chunk reorder via 503 retry → corrupted object reported as success |
| [kv-source-mirror-missing-subject-transforms](kv-source-mirror-missing-subject-transforms.md) | KeyValue | Sourced entries invisible; mirror buckets unread/unwritable |
| [pull-engine-503-terminal-silent-stop](pull-engine-503-terminal-silent-stop.md) | JetStream pull | Infinite consumer dies silently on transient 503 |
| [fetchbatch-inbox-not-slow-consumer-exempt](fetchbatch-inbox-not-slow-consumer-exempt.md) | JetStream fetch | Reply bursts silently dropped (permanent loss on max_deliver=1) |
| [srv-metadata-empty-array-vs-object](srv-metadata-empty-array-vs-object.md) | Services | `[]` vs `{}` breaks Go tooling (nats micro ls) |
| [service-reply-failures-escape-dispatch](service-reply-failures-escape-dispatch.md) | Services | Reply failures escape into shared dispatch loop (one remotely triggerable) |

## Minor

| Issue | Area |
|---|---|
| [ws-midbatch-decode-throw-discards-frames](ws-midbatch-decode-throw-discards-frames.md) | WS transport |
| [ws-frame-strictness-gaps](ws-frame-strictness-gaps.md) | WS transport (incl. silent corruption via fragmented control frames) |
| [ws-handshake-validation-gaps](ws-handshake-validation-gaps.md) | WS transport |
| [os-put-failure-orphans-chunks](os-put-failure-orphans-chunks.md) | ObjectStore |
| [os-addlink-overwrites-live-object](os-addlink-overwrites-live-object.md) | ObjectStore |
| [kv-os-enumeration-no-direct-get-fallback](kv-os-enumeration-no-direct-get-fallback.md) | KV/ObjectStore |
| [os-watch-pattern-base64url](os-watch-pattern-base64url.md) | ObjectStore |
| [ordered-consumer-stale-sid-handle](ordered-consumer-stale-sid-handle.md) | JetStream ordered consumer |
| [jetstream-client-errors-bypass-logger](jetstream-client-errors-bypass-logger.md) | JetStream observability |
| [directget-multi-last-chunk-404](directget-multi-last-chunk-404.md) | JetStream direct get |
| [push-consumer-no-adr9-gap-detection](push-consumer-no-adr9-gap-detection.md) | JetStream push consumers |
| [pull-inbox-permission-rejection-silent](pull-inbox-permission-rejection-silent.md) | JetStream pull / permissions |
| [headers-value-trimming](headers-value-trimming.md) | Core headers |
| [headers-name-case-lookup](headers-name-case-lookup.md) | Core headers |
| [service-endpoint-name-validation](service-endpoint-name-validation.md) | Services |
| [schema-validator-object-accepts-list](schema-validator-object-accepts-list.md) | Services |

## Suggested triage order

1. `ws-close-write-wedge` + `ws-unguarded-pong-write-drops-frames` + `transport-write-no-timeout-drain-flush-unbounded` (one transport-hardening batch)
2. `pull-engine-503-terminal-silent-stop` + `fetchbatch-inbox-not-slow-consumer-exempt` (both small, high-impact JetStream consume fixes)
3. `os-put-503-retry-chunk-reorder` + `os-put-failure-orphans-chunks` (upload integrity batch)
4. `os-watch-no-idle-heartbeat` (one-line parity fix), then `kv-os-watch-no-gap-detection` (larger: ordered-consumer-based watches)
5. `srv-metadata-empty-array-vs-object` + `service-reply-failures-escape-dispatch` (Services interop batch)
6. `kv-source-mirror-missing-subject-transforms` (KV sources/mirror redesign)
7. Remaining minors opportunistically.
