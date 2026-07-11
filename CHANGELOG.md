# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/).

Each entry is tagged so the version impact is clear:

- `[bugfix]` - corrects wrong behavior. Bump the patch version.
- `[feature]` - adds new, backward-compatible capability. Bump the minor version.
- `[docs]` - documentation only, no code change. No release needed (or patch).
- `[bc-break]` - changes existing behavior in a way that can break callers. Bump the major version.

Note on flags: a `[bc-break]` that only corrects an evident bug is treated as a
`[bugfix]`, not a real break, even though observable behavior changes.

## [Unreleased]

### Fixed

- `[bugfix]` An exception while handling one inbound frame no longer discards the frames already
  parsed from the same chunk. The parser has consumed the bytes, so an undispatched trailing frame
  was silently and permanently lost (core NATS does not resend) - e.g. a slow-consumer overflow on
  one subscription (Error policy) destroyed messages for healthy sibling subscriptions delivered
  in the same TCP chunk, and a failing PONG reply destroyed the messages behind the server PING.
  Dispatch is now contained per frame: every frame is handled, buffered deliveries are drained,
  and the first failure surfaces afterwards. The heartbeat self-read additionally reports a fatal
  frame (e.g. a server `-ERR`) through the `errorListener` instead of swallowing it whole (#128).
- `[bugfix]` Every terminal transition to `Closed` now releases per-connection runtime state
  (subscription registry and handler closures, queued messages, counters, parser bytes, reconnect
  buffer) - previously only user `disconnect()`/`drain()` did. An exhausted reconnect or a
  terminal auth failure left everything referenced; worse, calling `connect()` again on the same
  instance silently believed it was subscribed (nothing was re-SUBbed) and a later automatic
  recovery would resurrect the dead epoch's sids as ghost subscriptions, duplicating deliveries
  into stale handler closures. Subscriptions now never survive a terminal close: re-`connect()`
  starts from a clean slate and the application re-creates its subscriptions (nats.go parity,
  documented on `connect()`) (#127).
- `[bugfix]` A `NatsConnection` abandoned without `disconnect()`/`drain()` is now garbage-collectable:
  the ping timer's repeat closure previously bound `$this` strongly, so the event loop rooted the
  whole connection graph forever - the open socket, every subscription handler closure, and all
  buffers leaked per abandoned client, and the zombie timer kept PINGing and even delivering
  messages to abandoned handlers (stealing queue-group deliveries from live workers). The timer
  now holds the connection through a `WeakReference` and cancels itself once the application drops
  its last reference, and a new destructor cancels the heartbeat and closes the socket
  best-effort (#126).
- `[bugfix]` The reconnect handshake now starts from a clean protocol parser. Previously the new
  connection's INFO/PONG bytes were fed into the parser state left by the dead connection; after a
  drop mid-message-payload the pending frame swallowed each attempt's INFO as phantom payload
  bytes, so every reconnect attempt failed with "Expected INFO during connect" and the client
  closed permanently against a healthy server once the attempt budget was exhausted (#125).
- `[bugfix]` Transport `write()` on a closed/never-connected socket now throws
  `TransportClosedException` instead of silently succeeding (both TCP and WebSocket transports).
  Previously a publish, JetStream ACK, or flow-control reply racing a reconnect (or a concurrent
  `disconnect()`) could hit the nulled socket and report success while sending nothing - a silent
  message loss. The connection now also leaves the `Open` state before recovery's first await, so
  publishes issued while the dead socket is being torn down are routed into the reconnect buffer
  and replayed after the new handshake instead of racing the closing socket (#124).
- `[bugfix]` A reconnect-flush failure no longer silently destroys publishes accepted during the
  reconnect window: `flushReconnectBuffer()` cleared the buffer before awaiting the write, so a
  socket failure during the flush left the next (successful) attempt with nothing to replay while
  every affected `publish()` had already reported success. The buffer is now cleared only after
  the flush write succeeds (a partially transmitted flush may duplicate frames on the retry -
  duplication beats loss, matching nats.go pending-buffer semantics), and exhausting reconnect
  attempts with a non-empty buffer reports the abandoned bytes through the `errorListener` and
  clears the buffer so a later manual `connect()` cannot replay frames from a dead epoch (#123).
- `[bugfix]` `unsubscribe($sid, $max)` (auto-unsubscribe) sent `UNSUB <sid> <max>` but dropped the local
  handler immediately, so every message the server legitimately kept delivering up to the max was
  silently discarded. The handler now stays registered until `$max` total messages have been received
  (nats.go `AutoUnsubscribe` parity), a reconnect re-arms the server with the remaining allowance, and
  reaching the max removes the subscription locally (#112). The accounting is anchored to messages
  **received** (not delivered), so a message dropped by the slow-consumer policy still advances toward
  the max exactly as the server counts it - without this, under the default `DropOldest` policy the
  subscription could stall below its max forever (a permanent leak) and a reconnect would re-arm and
  over-deliver live messages past the intended max. Handler delivery is separately capped at the max so
  a batched-in or replayed extra frame is never over-delivered, and arming while a reconnect is in
  flight now defers to recovery instead of destroying the subscription (which would have silently lost
  the remaining armed deliveries).
- `[bugfix]` Subscription state no longer leaks on failure paths: `unsubscribe()` releases local state
  even when the connection is not open or the UNSUB write fails (previously it threw first, leaking the
  entry and its handler closure, and `resubscribeAll()` would revive dead inboxes as ghost
  subscriptions), and `subscribe()` rolls its registry entry back when the SUB write fails.
  `unsubscribe()` on a connection that is not open now cleans up silently instead of throwing
  `ConnectionException` - a bug-driven behavior change treated as a bugfix (#116).
- `[bugfix]` A failed ordered-consumer recreate (after a sequence gap) is no longer silently swallowed:
  the create is retried up to 3 times with backoff and a terminal failure is surfaced through the
  configured `errorListener`, so the application learns the consumer went permanently silent instead of
  waiting on dead air forever (#114). Adds `NatsClient::options()` exposing the client's runtime options.

## [2.4.1] - 2026-06-15

### Testing & CI

- `[docs]` Added mutation testing with [Infection](https://infection.github.io/) (`composer infection`,
  `scripts/run-mutation.sh`, `infection.json5`). 517 new unit tests under `tests/Unit/Mutation/` raised the
  suite's mutation score (Covered MSI) from **75% to 93%**, killing ~870 previously-surviving mutants. CI
  now enforces a **strict mutation gate** (a dedicated `mutation` job that fails the build below 90% MSI) -
  a quality bar on top of line coverage that catches assertions which pass but don't actually pin behavior.
  Mutation runs against the fast `unit` testsuite (no Docker); the remaining ~6% are mutants verified to be
  equivalent (no observable behavioral difference), documented in each test's reasoning rather than chased
  with meaningless assertions. Dev-only - no runtime/library change. **PHP 8.2 support is unchanged:**
  Infection requires PHP 8.3+ and is intentionally not in `require-dev`, so `composer install` still works
  on PHP 8.2; the `mutation` CI job runs on PHP 8.3 and installs Infection itself.

### Documentation

- `[docs]` Added a "PHP Support Policy" section to the README: the library follows PHP's official release
  schedule, so PHP 8.2 support will be dropped by the end of 2026 (when PHP 8.2 reaches end-of-life),
  raising the minimum to PHP 8.3 at that point.
- `[docs]` Pointed the "Made in the EU" badge at the renamed `ideaconnect/made-in-the-eu` repository.
- `[docs]` Replaced AI-style typography in the README, CHANGELOG, TESTS.md, and PHP docblocks/comments
  with plain ASCII: em/en dashes become `-`, the ellipsis character becomes `...`, and arrow / `x` for the
  arrow and multiplication signs. Functional non-ASCII (the `µs` duration unit, `©`, `§`) is left intact.

## [2.4.0] - 2026-06-14

### Added

- `[feature]` Protocol parser now recognizes operation verbs case-insensitively and accepts any whitespace
  (space or tab) between a verb and its arguments, aligning with the NATS wire spec. Real servers always
  send upper-case verbs, so this only adds leniency; argument/payload bytes are preserved verbatim. Resolves
  the long-standing README TODO (which has been removed).

### Fixed

- `[bugfix]` JetStream: `createStream()` no longer rejects an empty `subjects` list when a non-empty
  `sources` configuration is provided. A pure aggregate/sourcing stream legitimately has no subjects of its
  own (the server allows it); the client previously only exempted `mirror`, so creating a sources-only
  aggregate stream failed with "Stream subjects must not be empty...".
- `[bugfix]` Connection: a malformed async `INFO` frame is no longer allowed to throw out of the core
  `processIncoming()` read loop. Previously a non-JSON async INFO (corruption in flight, or a non-conformant
  server push) raised an uncaught `JsonException` that aborted the read cycle and skipped delivery of the
  `MSG` frames parsed from the same chunk. The runtime INFO decode is now contained (the bad update is
  skipped and surfaced via the error listener), mirroring the dispatch-containment principle from #97.
  Handshake INFO is still validated strictly and fails the connect on bad JSON.
- `[bugfix]` WebSocket transport: the frame decoder no longer re-slices the entire remaining receive buffer
  once per frame. A single read carrying many coalesced frames is now decoded in O(total bytes) instead of
  O(frames x bytes) by advancing a cursor and trimming once, improving throughput under bursty high-fanout
  traffic. Behavior (including the "leave an incomplete trailing frame buffered" contract) is unchanged.

### Documentation

- `[docs]` Added `TESTS.md` - a catalogue of every unit, integration, and Behat test with a one-line
  description of what it verifies - linked from the README's test baseline section.
- `[docs]` Added an `examples/` directory: one runnable, self-contained script per README example (42
  files), plus `scripts/run-examples.sh` which runs them all against dockerized NATS and reports
  pass/skip/fail - a gate that keeps the README examples honest. Linked from the README Usage section.
- `[docs]` Distributed Counter example now creates its backing stream with `allow_direct: true` (required
  because `counterValue()` reads via Direct Get); without it the documented example threw "no responders
  for $JS.API.DIRECT.GET". Prose updated accordingly.
- `[docs]` Each `examples/*.php` script now opens with a file-level intro docblock describing exactly what
  it does and which README section it mirrors.
- `[docs]` Every example heading in the README now carries a "Runnable example" pointer linking to the
  matching `examples/*.php` script, so each documented feature is one click from a runnable, verified file.
- `[docs]` `scripts/run-examples.sh` now defaults `NATS_NKEY_SEED` to the dev seed trusted by
  `build/nats/nkey.conf`, so `auth-standalone-nkey.php` runs as a real functional test in the dev stack
  instead of self-skipping. With this, all 42 examples pass against the full dockerized stack.
- `[docs]` CI now runs every example as a required gate (a dedicated `examples` job in
  `.github/workflows/ci.yml` that boots the full dockerized stack and runs `scripts/run-examples.sh`).
  The runner gained an `EXAMPLES_STRICT` mode (used by CI) that treats a skipped example as a failure, so
  the build fails unless every example actually executes and passes.

## [2.3.0] - 2026-06-13

### Security

- **Credential exposure via a configured `tlsContext` (#95).** Versions before 2.3.0 could transmit the
  CONNECT frame - which carries the configured credentials (token / user-password / JWT signature / NKey
  signature) - in **cleartext** when a `NatsOptions::$tlsContext` was supplied but `tlsRequired` was off,
  the DSN used the `nats://` scheme, and the server's INFO did not advertise `tls_required`. The TLS-required
  check ignored `tlsContext`, so the upgrade and the cleartext fail-safe were both skipped. Fixed: a
  configured `tlsContext` now forces the TLS upgrade (and fails fast if TLS cannot be established).
  **Upgrading is recommended for anyone using the `tlsContext` escape hatch.** See the Fixed entry below.

### Added

- `[feature]` Object Store: `ObjectStoreBucket::watch()` now accepts an optional `ObjectStoreWatchOptions`
  to select the delivery policy, mirroring the KeyValue watch matrix and the reference ObjectStore.Watch.
  With no options (`null`) the watcher stays updates-only (`deliver_policy=new`, unchanged). Passing an
  `ObjectStoreWatchOptions` instance opts into "snapshot then follow" - replay the current metadata of
  every existing object first, then live updates (`last_per_subject`, the reference default) - or full
  history (`includeHistory`) / explicit updates-only (`updatesOnly`). (#98)
- `[feature]` Services: a declared endpoint `schema` is now also surfaced in the standard `$SRV.INFO`
  response endpoint entries. ADR-32 stabilizes only PING/INFO/STATS, so spec-conformant tooling (nats CLI
  micro, nats.go) never queries the non-spec `$SRV.SCHEMA` verb; carrying the schema in INFO makes it
  discoverable. The `$SRV.SCHEMA` verb is retained for backward compatibility. (#101)

### Fixed

- `[bugfix]` Object Store: an object stored with empty/default metadata is now readable by the official
  NATS clients. Empty `metadata` was serialized as a JSON array (`"metadata":[]`), which the Go client
  rejects with "object-store meta information invalid" because it expects a `map`; the field is now omitted
  when empty (matching `omitempty`), restoring interoperability with the `nats` CLI / nats.go for the
  common default-metadata case. Verified live against the `nats` CLI. (#109)
- `[bugfix]` KeyValue: `watch()`'s `onCaughtUp` (end-of-initial-data) signal now fires on an empty or
  no-match bucket. Previously it could only fire from a delivered message reporting `num_pending = 0`, so
  with nothing to deliver it never fired and a caller blocking on it hung forever. The signal is now also
  derived from the created consumer's `num_pending` and fires immediately when the consumer starts with
  nothing pending. (#99)
- `[bugfix]` Protocol: the inbound MSG/HMSG frame bound is now coupled to the server's negotiated
  `max_payload` instead of a fixed 8 MiB. On a server with a raised `max_payload` (e.g. 16/32/64 MiB), a
  legitimately large message larger than 8 MiB was rejected as an oversized frame - throwing a
  `ProtocolException` that the connection turned into a reconnect, so the message was effectively
  undeliverable. The parser bound is raised from INFO (`max_payload` + a header-block margin, never below
  the historical 8 MiB), with a generous 64 MiB fallback when `max_payload` is unknown. (#94)
- `[bugfix]` Services: the endpoint success path no longer lets a `json_encode` failure escape the shared
  dispatch loop. A handler returning a value that cannot be JSON-encoded (binary / non-UTF-8 data, NAN/INF)
  previously threw a `JsonException` out of the subscription callback, aborting delivery for every
  subscription on the connection. The response publish is now guarded: an encode failure is recorded and
  answered with a controlled `HANDLER_ERROR`/500 reply (mirroring the handler-exception path), so one
  endpoint returning binary data can no longer take down the whole client's dispatch. (#97)
- `[bugfix]` KeyValue: `history()` no longer uses the throwing `messageMetadata()` path. A delivery
  lacking a parseable `$JS.ACK` reply subject (a control / non-conformant frame) is now skipped instead of
  throwing out of the shared dispatch loop - which would tear down delivery for every subscription on the
  connection (the same class fixed for `watch()` in #90) - and is no longer recorded as a bogus history
  entry. (#96)
- `[bugfix]` TLS: a configured `NatsOptions::$tlsContext` now correctly forces the TLS upgrade, matching
  its documented "treated as TLS-required" contract. Previously `requiresTls()` ignored `tlsContext`, so
  a `tlsContext`-only configuration over a `nats://` DSN to a server that did not advertise `tls_required`
  connected in plaintext and wrote CONNECT (carrying credentials) in cleartext. The credentials fail-safe
  now also covers this path, so a `tlsContext` whose handshake cannot establish TLS fails fast instead of
  leaking credentials. (#95)
- `[bugfix]` WebSocket: a corrupt permessage-deflate frame no longer emits an uncaught native `E_WARNING`
  from `inflate_add()`/`deflate_add()` before the typed `ProtocolException`. The warning is now suppressed
  (the return-value check already raises `ProtocolException`), so apps that promote warnings to exceptions
  get the intended `ProtocolException` instead of a generic `ErrorException` leaking from the codec. (#100)

### Documentation

- `[docs]` README "Reconnect Behavior" no longer wrongly states that publishes during reconnect are lost.
  It now documents the outbound reconnect buffer (publishes are buffered up to `reconnectBufferSize`,
  default 8 MiB, and flushed on reconnect; rejected only when the buffer is full, buffering is disabled, or
  the connection is closed/not reconnecting), with test citations. (#102)
- `[docs]` README "Configuration Option Mapping" table now lists the 11 previously-omitted `NatsOptions`
  fields - `connectionListener`, `errorListener`, `jwtProvider`, `tokenProvider`, `reconnectBufferSize`,
  `tlsContext`, `randomizeServers`, `retryOnFailedInitialConnect`, `webSocketHeaders`,
  `webSocketCompression`, `logger` - with types/defaults. `NatsOptionsTest::testDefaultsMatchDocumentedValues`
  now asserts these defaults too, keeping the table's "asserted by" claim accurate. (#103)
- `[docs]` README: new "WebSocket Transport" section (with an Index entry) showing how to wire
  `WebSocketTransport`, the `ws://` / `wss://` expectations, and the `webSocketHeaders` /
  `webSocketCompression` options. (#104)
- `[docs]` README: the Observability note now documents the typed `connectionListener` /
  `errorListener` closures, not just the PSR-3 logger. (#105)
- `[docs]` README: added a standalone-NKey authentication example (nkey + nonceSigner, no JWT) to the
  Authentication Options block. (#106)
- `[docs]` PHPDoc: `KeyWatchOptions` and `KeyValueBucket::watch()` now make clear that the
  last-per-subject "snapshot then follow" default applies only when a `KeyWatchOptions` instance is
  supplied; `watch()` called with `$options = null` is updates-only and replays nothing. (#107)
- `[docs]` PHPDoc: `ObjectInfo::$digest` is no longer described as "Server-provided" - it is the content
  digest recorded by the writing client and verified on read. (#108)
- `[docs]` Added a runnable performance baseline script (`scripts/benchmark.php`, request/reply + publish
  throughput) and a sample-results table in the README's Performance section.

## [2.2.0] - 2026-06-10

### Added

- `[feature]` Server-version awareness for version-gated features. Each feature's minimum NATS version
  is documented (PHPDoc `Requires NATS X.Y+` notes + a compatibility table in the README) and exposed
  programmatically via the new `IDCT\NATS\JetStream\FeatureSupport` registry
  (`FeatureSupport::requiredVersion('allow_atomic')` -> `"2.12"`).
- `[feature]` New `IDCT\NATS\Exception\UnsupportedFeatureException` (a subclass of `JetStreamException`).
  When a JetStream request fails because the connected server is too old for a feature (the server
  rejects the config field with `unknown field "X"`), the client now raises this typed exception
  carrying the feature, the required version, and the server's reported version - instead of an opaque
  error. The detection is **reactive** (derived from the server's own response on failure); there is no
  per-request version probe. `JetStreamException` is no longer `final` so it can be specialized
  (existing `catch (JetStreamException)` handlers are unaffected).

## [2.1.1] - 2026-06-10

Verification pass for the 2.1.0 roadmap features against a live NATS 2.12.9 server.

### Fixed

- `[bugfix]` Atomic batch publish (#8): the stream-config field is **`allow_atomic`** - the server
  rejects the previously-documented `allow_atomic_publish` with `unknown field`. Corrected the
  `batch()`/`BatchPublisher` docblocks (the `BatchPublisher` code itself was already correct and is now
  verified end-to-end: a 3-message batch commits 3/3 with the `batch`/`count` ack parsed).

### Added

- Live integration tests for atomic batch publish (#8) and batched/multi Direct Get (#13), and a
  connection-level regression test for the fragmented-INFO handshake (#2, the `trim($chunk)` bug fixed
  in v1.0.1 previously had no test). Full integration suite (76 tests) passes against NATS 2.12.9.

## [2.1.0] - 2026-06-10

NATS 2.11/2.12 client feature support (roadmap milestone, GitHub issues #4-#14). All changes are
backward compatible (new optional parameters / new methods); the one behavior change (#5 delete
markers) is bug-driven and flagged `[bugfix]`, so this is a minor release.

### Changed

- `[bugfix]` Honor JetStream subject delete-markers (`Nats-Marker-Reason`: MaxAge/Remove/Purge, ADR-43,
  issue #5). A server-written delete-marker is now treated as a tombstone rather than a live value:
  `KeyValueBucket::get()` returns a `PURGE` entry with a null value (was an empty-string `PUT`),
  `getAll()` omits the key, `watch()` emits a tombstone, and `ObjectStoreBucket::watch()`/`info()` skip
  the marker. **Behavior change** (flagged `bc-break` on the issue, but bug-driven so versioned as a
  bugfix): only reachable when a stream has `subject_delete_marker_ttl` set, which this client now also
  forwards as a create option.

### Added

- `[feature]` Batched / multi Direct Get (ADR-31, issue #13). New `directGetBatch()` collects a
  multi-response Direct Get stream (terminated by a 204 EOB or `Nats-Num-Pending: 0`), and
  `directGetLastForSubjects()` fetches the latest message for many subjects in one request via
  `multi_last`. Additive - the existing per-subject bulk paths (`getAll()`/`list()`) are unchanged
  pending live verification on a 2.11+ server.
- `[feature]` Pull-consumer priority groups and richer pull options (ADR-42, issue #7).
  `fetchBatch()`/`fetchNext()` accept a `$pull` array (`group`, `id`, `min_pending`,
  `min_ack_pending`, `priority`, `max_bytes`, `no_wait`); `PullConsumerIterator` gains
  `setGroup()`/`setPriority()`/`setMinPending()`/`setMinAckPending()`/`setMaxBytes()`/`setNoWait()`
  and transparently captures the `Nats-Pin-Id` and re-pins on a 423 stale-pin status. New
  `unpinConsumer()` (CONSUMER.UNPIN) and `pinIdOf()`; consumer-create validates `priority_groups`/
  `priority_policy`.
- `[feature]` Atomic (all-or-nothing) batch publish (ADR-50, issue #8). `JetStreamContext::batch()`
  returns a `BatchPublisher`: `add()` stages messages and `commit()` sends them with a shared
  `Nats-Batch-Id`, an incrementing `Nats-Batch-Sequence`, and `Nats-Batch-Commit: 1` on the final
  message, returning a single PubAck exposing the committed `batchCount`/`batchId`. Capped at 1000
  messages; an aborted batch surfaces as a `JetStreamException`. Requires `allow_atomic_publish` on
  the stream.
- `[feature]` Multi-subject consumer filters (issue #10, NATS 2.10+). The consumer-create methods now
  accept a `filter_subjects` array (via options), validated client-side and mutually exclusive with the
  singular filter subject (combining the two is rejected with a clear error instead of an opaque server
  rejection).
- `[feature]` Distributed counter CRDT (ADR-49, issue #9). `JetStreamContext::incrementCounter()`
  publishes a `Nats-Incr` delta (signed/unsigned integer string) and returns the new total;
  `counterValue()` reads the current value via Direct Get ("0" when absent). Values are handled as
  strings (decoded with `JSON_BIGINT_AS_STRING`) so arbitrary-precision counters are not truncated.
  The target stream must be created with `allow_msg_counter` enabled.
- `[feature]` `JetStreamContext::publish()` now accepts optional message headers - a generic
  `array $headers`, a `$msgId` (`Nats-Msg-Id`) for server-side de-duplication within the stream's
  `duplicate_window` (issue #11), and a per-message `$ttl` (`Nats-TTL`; requires `allow_msg_ttl` on
  the stream - issue #4). `KeyValueBucket::put()` takes an optional per-key `$ttl`, and
  `delete()`/`purge()` take an optional tombstone TTL. TTL values (integer seconds, a Go duration
  string, or "never") are validated client-side via the new `MessageTtl` helper.
- `[feature]` Recurring and cron scheduled publishing (ADR-51, issue #6). `Schedule::every()` builds
  an `@every <interval>` expression (from an integer number of seconds or a Go-style duration string)
  and `Schedule::cron()` validates/returns a 6-field (seconds-resolution) cron expression.
  `Schedule::predefined()` returns a predefined alias (`@daily`, `@hourly`, ...).
  `JetStreamContext::publishScheduled()` now accepts `@at` (with `Z` or a numeric RFC3339 offset),
  `@every`, cron, and the predefined aliases (previously only `@at` with `Z`) and emits the optional
  `Nats-Schedule-Source`, `Nats-Schedule-Time-Zone` (cron/alias only, rejected otherwise), and
  `Nats-Schedule-Rollup: sub` headers alongside the existing
  `Nats-Schedule`/`-Target`/`-TTL`. The target stream must be created with `allow_msg_schedules`
  enabled (e.g. `createStream(..., ['allow_msg_schedules' => true])`).

## [2.0.0] - 2026-06-07

Findings from a deep review against a live NATS 2.12 server (README correctness,
real-server behavior, bugs, and performance). Object Store interoperability with
the `nats` CLI, idle-connection heartbeat survival, and request-timeout recovery
were all verified working and are unchanged.

### Fixed

- `[feature]` Added `flush()` (on `NatsClient`/`NatsConnection`): sends a PING and waits for the
  server's PONG, confirming the server has processed everything written so far (e.g. a SUBSCRIBE
  before publishing a dependent request). Bounded by the request timeout.
- `[feature]` Service endpoints accept optional per-endpoint `metadata` (`addEndpoint(..., metadata:)`),
  advertised in the `$SRV.INFO` response per the NATS micro spec.
- `[bugfix]` The protocol parser now rejects a size/sid token that would overflow a PHP int (which
  `(int)` silently saturates to `PHP_INT_MAX`) as a `ProtocolException`.
- `[bugfix]` `NkeySeedSigner` now zeroes the raw seed and key-pair buffers (`sodium_memzero`) once the
  Ed25519 key is derived; `ProtocolCodec` fails fast if a configured nkey does not match the seed
  signer's public key. The service `started` timestamp now carries sub-second precision.
- `[bugfix]` `ObjectStoreBucket::putStream()` no longer recopies the buffer tail per chunk (O(n^2)
  for a producer block much larger than `chunkSize`); it advances a read offset and compacts once per
  block. The constructor now rejects a non-positive `chunkSize` (which made `put()`/`putStream()` loop
  forever) with a `JetStreamException`.
- `[bugfix]` Object Store `info()`/`get()`/`list()` now populate `ObjectInfo::revision` from the
  record's stream sequence (the `Nats-Sequence` Direct Get header, or the `seq` of the STREAM.MSG.GET
  fallback) instead of always leaving it null.
- `[bugfix]` KeyValue/Object Store bucket names are now validated (`^[A-Za-z0-9_-]+$`); a name with
  dots or wildcards would otherwise mis-scope the backing stream subjects.
- `[bugfix]` JetStream `publish()`/`publishScheduled()` now translate a no-responders reply into a
  `JetStreamException` (code 503) - e.g. publishing to a subject not bound to any stream - so a
  `catch (JetStreamException)` no longer misses it as a bare `NatsException`.
- `[bugfix]` `drain()` now always closes the socket and clears state even if a fatal frame surfaces
  mid-flush, instead of escaping the flush loop and leaving the connection wedged in `Draining`.
- `[bugfix]` Reconnect no longer deadlocks when a subscription handler publishes during recovery.
  Subscription-replay (`drainImmediateServerFrames`) previously delivered buffered messages to user
  callbacks while still inside the reconnect critical section; a callback that published and hit a
  write failure re-entered `recoverConnection()` and awaited the in-progress reconnect, hanging the
  recovery fiber. Buffered messages are now delivered after recovery completes (outside the critical
  section), so such a callback starts a fresh recovery instead of deadlocking.
- `[bugfix]` Microservice endpoint error replies now carry the NATS micro-spec `Nats-Service-Error`
  and `Nats-Service-Error-Code` reply headers (400 for validation, 500 for handler errors), so a
  generic client (Go `micro`, `nats` CLI) detects the failure by header instead of treating the
  header-less JSON error body as success. The description is collapsed to a single line so a crafted
  message cannot break header framing; the JSON error body is unchanged.
- `[bugfix]` KeyValue `getAll()` now paginates the STREAM.INFO subjects map (via `offset`) instead
  of reading a single page, so a bucket with more keys than the server's subjects-map cap is no longer
  silently truncated (mirroring Object Store `list()`), and it now throws on a STREAM.INFO API error
  instead of swallowing it into an empty result.
- `[bugfix]` Single-record Direct Get reads now fall back to the leader `STREAM.MSG.GET` path
  when Direct Get is unavailable (a stream with `allow_direct` disabled, or an older server). The
  no-responders error is translated to a clear `JetStreamException` (code 503); KeyValue `get()` and
  Object Store `info()`/`get()` (single-chunk fast path) then retry on the leader, so reads keep
  working on interop buckets (e.g. created by the `nats` CLI without `allow_direct`) instead of
  surfacing an opaque error.
- `[bugfix]` Ordered-consumer gap detection and `streamSequenceOf()` (used for KV/Object Store
  revision) now parse the 11-token domain-qualified ACK reply subject
  (`$JS.ACK.<domain>.<account>.<stream>...` without the trailing random token), not just the 9- and
  12-token forms. Previously the sequence fell through to null on JetStream-domain/leaf deployments,
  silently disabling gap detection and revision tracking there.
- `[bugfix]` The plaintext-credentials fail-safe now also covers the handshake-first TLS path. The
  guard that refuses to write CONNECT (which carries jwt/sig/nkey/user/pass/token) over a still-plain
  socket was gated on the non-handshake-first branch, so `tlsHandshakeFirst=true` combined with no TLS
  materials (and a `nats://` DSN) while the server's INFO advertised `tls_required` could leak
  credentials in cleartext. The fail-fast now runs whenever TLS is required and the handshake did not
  establish it, regardless of `tlsHandshakeFirst`.
- `[bugfix]` A JetStream flow-control STALL heartbeat is now answered. The server leaves the message
  reply empty for a stall and puts the flow-control reply subject in the `Nats-Consumer-Stalled`
  header value; the client previously detected the stall but published to the empty reply, so the
  ack never reached the server and a throttled ordered/flow-controlled consumer could stall
  indefinitely with no error surfaced. The normal `$JS.FC.` flow-control-request reply path is
  unchanged.
- `[bugfix]` Subscription dispatch is now non-reentrant per SID. If a handler awaits on the
  connection (e.g. an ordered consumer recreating itself during gap recovery), a heartbeat tick
  or a nested `request()` self-pump could previously re-enter the per-SID drain and deliver that
  subscription's next message on top of the still-suspended handler - corrupting ordered-consumer
  recovery state (stale by-reference sequence/consumer-name -> duplicate `deleteConsumer`/recreate)
  and causing overlapping/duplicate delivery. Delivery for a SID in flight is now deferred until
  the suspended handler returns (FIFO preserved); other SIDs stay deliverable so nested requests
  still complete.
- `[bugfix]` The CONNECT frame now advertises the resolved client library version (from the
  installed Composer package, with a constant fallback) instead of the stale hardcoded
  `0.1.0-dev`, so server `connz`/monitoring attributes traffic to the correct version.
- `[bugfix]` Header publishes (`publishWithHeaders()`, KV/Object Store metadata writes) now
  build and CR/LF-validate the header wire block once and reuse it for sizing and each write
  attempt, instead of re-running `toWireBlock()` two or three times per publish.
- `[bugfix]` The per-chunk subscription drain no longer rescans every subscription that has
  ever received a message: a drained (or undeliverable) per-SID queue is now released, so the
  drain stays proportional to the subscriptions with pending messages. This also fixed a latent
  coupling where the request UNSUB cleanup was gated on the (now-released) pending queue.
- `[bugfix]` Object Store `get()`/`getToCallback()` now fetch a single-chunk object with one
  Direct Get on its chunk subject, instead of creating, pulling from, and deleting a transient
  ephemeral consumer - turning the common small-object download from 4 round-trips into 1 (plus
  the metadata read). Multi-chunk objects still use the batched pull-consumer path.
- `[bugfix]` Object Store `put()` and `delete()` now run the previous-revision lookup
  concurrently with the chunk upload / tombstone publish, awaiting it only just before the
  best-effort chunk purge it feeds. Previously the lookup was a serial round-trip on the
  critical path before the first byte was written, roughly doubling small-object write
  latency. The lookup is issued at the same coroutine depth as the upload, so request
  ordering stays deterministic.
- `[bugfix]` Ephemeral push consumers (KeyValue/Object Store `watch()`, ordered consumer)
  now set an `inactive_threshold`, so the server reaps them once the subscription ends
  instead of leaking server-side consumers when a long-running app re-subscribes. An active
  subscription keeps the consumer alive; callers may override the threshold.
- `[bugfix]` `SubscriptionQueue` now bounds its polling backlog with
  `maxPendingMessagesPerSubscription` and the configured slow-consumer policy. Previously
  the connection's per-chunk drain emptied its (capped) queue into the unbounded polling
  queue, so a queue consumed slower than it was fed could grow until OOM.
- `[bugfix]` Object Store `list()` now paginates the meta-subject enumeration (via the
  STREAM.INFO `offset`) instead of reading a single page, so a bucket with more objects
  than the server's subjects-map cap is no longer silently truncated. The loop terminates
  on the first empty/duplicate page, so it is safe even against a server that ignores
  `offset`.
- `[bugfix]` `NatsHeaders::toWireBlock()` now rejects an empty/blank header name or one
  containing whitespace or a colon (previously only CR/LF were rejected, so a bad name
  silently produced a malformed/mutated block), and trims surrounding whitespace from
  values so they round-trip symmetrically with decode (which already trims).
- `[bugfix]` Object Store digest verification now compares the decoded digest *bytes*
  (tolerating missing base64url padding) with `hash_equals()`, instead of a string compare
  that spuriously rejected a byte-identical object whose metadata used unpadded base64url
  (some non-Go clients).
- `[bugfix]` KeyValue `get()`/`update()`/`delete()`/`purge()`/`getAll()` now wrap a
  malformed (non-JSON) reply in a `JetStreamException` instead of leaking a raw
  `JsonException`, consistent with `put()` and the rest of the API.
- `[bugfix]` The protocol parser now bounds an unterminated control line (no CRLF) to 1 MiB
  and raises a `ProtocolException` instead of buffering it without limit. `maxFrameSize`
  only bounded MSG/HMSG payloads (parsed after their control line completes), so a peer
  streaming bytes without a CRLF could drive the client to OOM.
- `[bugfix]` `Service::start()` is now atomic: if a subscribe fails partway, it rolls back
  the subscriptions already made and rethrows, instead of leaving the service
  half-initialized with the idempotency guard then masking a retried `start()` as a no-op.
  A separate `started` flag tracks completion.
- `[bugfix]` Microservice request observers now receive the terminal `request_end` event
  on the schema-validation rejection path too (previously only `request_start` ->
  `request_error` fired), so observer spans/timers/gauges are not leaked for rejected
  (often hostile) traffic.
- `[bugfix]` `NatsClient::service()` now validates the service name
  (`^[A-Za-z0-9_-]+$`) and requires a semantic version, failing fast instead of crashing
  `start()` mid-loop or over-subscribing to discovery subjects when the name contains a
  dot/space/wildcard.
- `[bugfix]` `request()` (and every JetStream/KV/Object Store call built on it) no longer
  throws a spurious `TimeoutException` when the reply is delivered in the same event-loop
  tick the deadline fires. The wait loop now checks for completion before the deadline, so
  a reply that lands as the timeout expires is returned instead of discarded.
- `[bugfix]` Ordered-consumer gap recovery now contains a failed consumer recreate
  (pruned/deleted stream, leadership change, transient timeout) instead of throwing out of
  the shared subscription dispatch loop and aborting delivery for every other subscription
  on the connection.
- `[bugfix]` `PullConsumerIterator` infinite mode (`setIterations(null)`) now survives a
  transient `409` (`Exceeded MaxAckPending`, `Leadership Change`, `Server Shutdown`,
  `Exceeded MaxWaiting`) and keeps polling, instead of treating every non-404/408 status
  as terminal and silently exiting forever. A terminal `409 Consumer Deleted` still stops
  the loop, and finite mode is unchanged.
- `[bugfix]` `drain()` no longer busy-spins (100% CPU) or hangs when the server never
  sends the flush PONG. The flush loop now yields between empty reads so its deadline can
  fire; previously a synchronous 0-frame read starved the event loop, so the
  `TimeoutCancellation` could never fire and `drain()` never returned.
- `[bugfix]` `drain()` no longer resurrects the connection on a read failure mid-flush. A
  peer close during drain previously triggered `recoverConnection()` - reconnecting and
  re-SUBscribing the very subscriptions `drain()` had just removed (and possibly
  re-delivering messages). `processIncoming()` now skips recovery while the connection is
  `Draining` and treats the read failure as end-of-flush.
- `[bugfix]` `CredentialsParser` now parses real `nsc`-generated `.creds` files. The
  marker regex required exactly five dashes on both the BEGIN and END lines, but the
  NATS toolchain emits five dashes on BEGIN and **six** on END, so
  `CredentialsParser::fromFile()` threw `Credentials file does not contain a NATS USER
  JWT block` on essentially every genuine credentials file - making the documented
  JWT-via-`.creds` auth path unusable. Both markers now accept five-or-more dashes.
- `[bugfix]` Object Store now stores a 0-byte object with `chunks=0` and publishes no
  chunk message, matching the official Object Store layout; previously it wrote one
  empty chunk and recorded `chunks=1`. `get()` of an empty object also returns
  immediately instead of blocking until the download batch expiry waiting for a chunk
  that never arrives.
- `[bugfix]` Object Store `get()` and `getToCallback()` now return `null` for a deleted
  (tombstoned) object, consistent with a missing object and the official not-found
  semantics; the tombstone metadata remains observable via `info()`. Previously `get()`
  returned an `ObjectData` with `null` data and `getToCallback()` returned the
  `ObjectInfo`.
- `[bugfix]` Microservice handler errors no longer leak the raw exception message to the
  requester: the reply carries a generic `Internal server error` under the
  `HANDLER_ERROR` code, while the full detail stays server-side (endpoint `lastError`,
  `$SRV.STATS`, and the `request_error` observer event).
- `[bugfix]` Service `$SRV.STATS` no longer emits the non-spec `requests`/`errors`
  aliases; only the spec-compliant `num_requests`/`num_errors` remain.
- `[bugfix]` Connections now disable Nagle's algorithm (`TCP_NODELAY`). NATS is a
  small-message request/reply protocol, and Nagle combined with delayed ACKs added
  roughly 40 ms of latency per round trip; local request/reply throughput improved
  about 16x in a single-process benchmark (~22 to ~365 req/s) after this change.
- `[bugfix]` Ordered consumers (`subscribeOrderedConsumer()`) now deliver in order,
  gap-free, and without duplicates. Gap detection was based on the stream sequence,
  which is non-contiguous for a filtered consumer, so every filtered delivery looked
  like a gap; and on a gap the out-of-order message was forwarded and the expected
  sequence advanced past it, causing duplicate/out-of-order delivery and a cascading
  consumer delete+recreate storm. Detection now uses the JetStream consumer
  (delivery) sequence, the out-of-order message is discarded, and the consumer is
  recreated from the last in-order stream sequence (resuming from the next available
  message if the restart point was pruned).
- `[bugfix]` `NatsOptions` now rejects genuinely-invalid configuration at construction
  (non-positive `connectTimeoutMs`/`requestTimeoutMs`, `maxPendingMessagesPerSubscription`
  below 1, and negative reconnect/`maxPingsOut` values) with an `InvalidArgumentException`,
  instead of misbehaving later. Legitimate edge values stay valid: `pingIntervalSeconds`
  `<= 0` disables the heartbeat, `maxPingsOut` 0 is allowed, and an empty `servers` list
  falls back to the default - so this is input validation, not a breaking change.
- `[bugfix]` KeyValue keys with a leading, trailing, or consecutive dot (which produce a
  malformed `$KV.<bucket>.<key>` subject) are now rejected up front; dots, colons and
  slashes elsewhere in a key remain valid.
- `[bugfix]` Object Store `put()` now pipelines chunk publishes in bounded in-flight
  windows instead of awaiting one PubAck round-trip per chunk, so large-object uploads
  are no longer strictly round-trip-bound. PUB frames are written to the single
  connection in chunk order, so stream order (and download reassembly) is preserved.
- `[bugfix]` Single-record reads - KeyValue `get()` and Object Store `info()` (and the
  metadata read behind `get()`/`getToCallback()`) - now use the Direct Get API (served by
  any replica) instead of leader-only `STREAM.MSG.GET`, consistent with `getAll()`/`list()`.
  On clustered/replicated streams this stops concentrating reads on the stream leader. (The
  internal put/delete cleanup lookup stays on `STREAM.MSG.GET` for deterministic ordering.)
- `[bugfix]` KeyValue `getAll()` and Object Store `list()` now read the latest record
  per key/object via the Direct Get API issued concurrently, instead of N+1 sequential
  leader-only `STREAM.MSG.GET` reads. For large buckets this stops hammering the stream
  leader and collapses O(keys) serial round-trips into roughly one round-trip of
  wall-clock. (Concurrent request/reply on a single connection is covered by a new
  integration test.)
- `[bugfix]` `publish()` and `publishScheduled()` now wrap a malformed (non-JSON)
  acknowledgment in a `JetStreamException` instead of leaking a raw `JsonException`,
  consistent with the other JetStream API calls.
- `[bugfix]` Direct Get now rejects an unrecognized response (no status line and no
  `Nats-Stream`/`Nats-Sequence` headers) with a `JetStreamException` instead of
  returning a garbage body, guarding against a non-conformant server/proxy.
- `[bugfix]` KeyValue `watch()` now delivers updates through a JetStream push consumer
  (`deliver_policy=new`, ack-free) so each entry carries its `revision` (the stream
  sequence). Previously it used a plain core subscription and always reported
  `revision=null`, so a watcher could never feed an entry back into `update()`/CAS.
  Live-updates-only semantics are unchanged.
- `[feature]` `JetStreamContext::streamSequenceOf()` returns the stream sequence of a
  JetStream-delivered message (from its `$JS.ACK` reply).
- `[feature]` `ObjectStoreBucket::putStream()` uploads an object from a producer callback
  without holding the whole payload in memory (the streaming counterpart to
  `getToCallback()`): blocks of any size are re-chunked to `chunkSize`, published in bounded
  in-flight windows, and the SHA-256 digest is computed incrementally.
- `[feature]` Object Store `watch()` now delivers updates through a JetStream push consumer
  (consistent with KeyValue `watch()`) and exposes each update's stream sequence via the new
  `ObjectInfo::$revision` field. Previously it used a plain core subscription that carried no
  sequence/revision. Live-updates-only semantics (`deliver_policy=new`, ack-free) are
  unchanged; `$revision` is `null` on `ObjectInfo`s from `get()`/`info()`/`list()`.
- `[bugfix]` `Service::stop()` now tolerates a closed/lost connection: it unsubscribes
  each endpoint best-effort and always clears its subscription state, instead of
  aborting on the first failure (which leaked the remaining subscriptions and broke a
  later `start()` restart).
- `[bugfix]` `Service::addEndpoint()` now rejects a duplicate subject with an
  `InvalidArgumentException` instead of silently overwriting the earlier endpoint and
  handler (which also under-reported them in INFO/SCHEMA/STATS).
- `[bugfix]` `Service::run()` now stops when the connection is unrecoverable (closed
  for good) and backs off interruptibly, instead of busy-spinning at ~50 Hz silently
  swallowing the error.
- `[bugfix]` The microservice discovery handler now swallows an encode/publish failure
  (e.g. invalid-UTF-8 metadata) instead of throwing out of the shared dispatch loop,
  which would abort delivery of buffered frames for other subscriptions.
- `[feature]` `NatsClient::state()` exposes the current connection state.
- `[feature]` `SubscriptionQueue::unsubscribe()` / `close()` cancel the queue's own
  subscription (convenience for `$client->unsubscribe($queue->sid)`).
- `[feature]` `AmpSocketTransport` now accepts `nats://` DSNs directly (self-normalizing
  to `tcp://`), so the transport is usable standalone, not only via the connection layer.
- `[bugfix]` Object Store downloads now use a no-ack (`ack_policy=none`) consumer. The
  read-only download previously used an explicit-ack consumer and acked each chunk; on
  a slow link an ack stalling past `ack_wait` triggered redelivery, which re-hashed a
  chunk and produced a spurious digest mismatch.
- `[bugfix]` Object Store downloads now fail on a truncated transfer (fewer chunks than
  the metadata declares) via a digest-independent completeness check, instead of
  silently returning a partial object when the metadata carries no digest.
- `[bugfix]` Object Store `watch()` now tolerates a malformed metadata payload (skips
  it) instead of throwing out of the dispatch loop, which would abort delivery of
  buffered frames for other subscriptions.
- `[bugfix]` `listStreams()` and `listConsumers()` now paginate through the JetStream
  LIST API (`offset`/`total`). Previously they read only the first page, silently
  truncating accounts with more than the server page size (256) of streams, or a
  stream with more than 256 consumers.
- `[bugfix]` `PullConsumerIterator` infinite mode (`setIterations(null)`) now keeps
  polling past routine empty windows (404/408) instead of terminating on the first
  idle gap, so a long-running worker is no longer killed by a quiet period. Terminal
  errors (e.g. 409 consumer deleted) still stop the loop, and finite mode is unchanged.
- `[bugfix]` The heartbeat watchdog now resets the outstanding-ping counter only when
  an actual PONG is received, not on any inbound bytes. Previously a server that
  stopped answering PINGs but kept trickling data (or a proxy replaying buffered data)
  never tripped `maxPingsOut`, defeating dead-link detection on busy connections.
- `[bugfix]` `drain()` now waits for the server's PONG (bounded by a deadline) before
  closing, instead of bailing on a transient partial/empty read. A larger message
  split across socket reads no longer cuts the flush short and drops in-flight
  deliveries.
- `[bugfix]` `SubscriptionQueue::fetchAll()` no longer returns early on a transient
  empty read (e.g. the heartbeat self-read briefly owning the socket) while its
  configured timeout window still has time remaining.
- `[bugfix]` `recoverConnection()` now coalesces concurrent reconnect attempts. A
  suspended ping-timer callback resuming while the read path already began recovering
  can no longer launch a second reconnect that races on the parser, state, and socket.
- `[bugfix]` The protocol parser now rejects malformed frames instead of silently
  misframing the stream: non-numeric or negative MSG/HMSG sizes and sids, and HMSG
  header bytes exceeding total bytes, raise a `ProtocolException`. A parse failure now
  resyncs past the offending bytes instead of leaving them buffered to re-throw on
  every subsequent read, and `processIncoming()` treats an unparseable stream as a
  transport failure (reconnect) rather than letting the exception escape the read loop.
- `[bugfix]` The client no longer transmits credentials in plaintext to a TLS-required
  server. When the server advertises `tls_required` (or the option/`tls://` scheme
  requires TLS) but no TLS materials were configured at connect time, the previous
  code performed a no-op "upgrade" and then wrote CONNECT (token/user/pass/JWT/sig)
  over the still-plaintext socket, hanging until the handshake deadline. The client
  now fails fast with a clear error and never writes CONNECT before TLS is active.
- `[bugfix]` A graceful peer close (socket EOF) now triggers reconnect from the read
  path. `readLine()` previously collapsed the EOF `null` into an empty string, which
  the connection treated as "no data this tick", so a server restart / idle-timeout /
  load-balancer reap left the client believing it was connected to a dead socket
  (never recovering when pings are disabled, recovering only after ~90s otherwise).
  Transports now signal EOF via a `TransportClosedException`, which `processIncoming()`
  and the heartbeat self-read escalate to `recoverConnection()`.
- `[bugfix]` `SubscriptionQueue::fetch()`, `next()` (with no/zero/negative timeout),
  and `fetchAll()` (with no timeout) no longer block the calling fiber forever on an
  idle subject against a real socket. Each now bounds its single poll with a small
  cancellation, honoring the documented non-blocking contract.
- `[bugfix]` `Service::run()` now passes its cancellation into `processIncoming()`,
  not only the outer `await()`. Previously a timed/cancelled run loop left the idle
  socket read running detached, wedging the shared connection (every later read
  short-circuited and the heartbeat stalled). The read is now torn down on cancel.
- `[bugfix]` `getStreamMessage()` no longer returns an empty payload when the stored
  body is the single character `0`. The decoded body was passed through a falsy
  fallback, so a legitimate `"0"` payload was replaced with an empty string.
- `[bugfix]` `getStreamMessage()` now preserves headers stored with the message.
  Previously the stored header block was dropped and the returned message had no
  headers.
- `[bugfix]` The heartbeat keep-alive read now delivers any application message it
  happens to read while consuming the server reply, instead of leaving it buffered
  until the next manual read. This removes a rare case where a reply could be
  delayed until its own timeout.
- `[bugfix]` Microservice endpoints now default to a shared queue group (`q`) so
  multiple instances of the same service load-balance requests, matching the NATS
  micro specification. Previously every instance handled every request, which
  duplicated side effects and work across instances. **Behavior change:** with more
  than one instance, each request is now handled by exactly one of them. Pass `null`
  or `''` as the endpoint queue group to opt out and fan out to all instances.
  (Reclassified from a breaking change to a bugfix because the previous behavior
  defeated the framework's scaling model.)

### Changed

- `[feature]` Faster Object Store downloads: object chunks are pulled in bounded
  batches rather than one request/reply round-trip per chunk, which significantly
  reduces latency for large, multi-chunk objects while keeping peak memory bounded.
  Digest verification, in-order delivery, the chunk-by-chunk `getToCallback()`
  contract, and `nats` CLI interoperability are unchanged.

### Documentation

- `[docs]` Corrected the Performance Benchmark Recipe, which previously stalled after
  roughly 50 requests because the responder consumed only one transport chunk per
  request. The recipe now drives the responder with a single continuous read loop,
  and the `processIncoming()` single-chunk semantics are spelled out.
- `[docs]` Corrected the Scheduled Publish example. It now creates the backing stream
  with `allow_msg_schedules` (and `allow_msg_ttl` when a schedule TTL is used) so the
  example runs as written; without those flags the server rejects the publish.
- `[docs]` Renamed the "Stream Message Direct Get" section to "Stream Message Get" and
  clarified that `getStreamMessage()` uses the standard stream message get API (not
  the JetStream direct-get API) and preserves the stored body and headers.
