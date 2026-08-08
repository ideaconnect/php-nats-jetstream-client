# Second-round review — 2026-08-08

Scope: the uncommitted working-tree diff implementing all 27 round-1 findings (~3,700 lines across
12 src files and 21 test files, baseline = HEAD). Reviewed for correctness, regressions,
NATS.io/ADR spec parity, code quality, and test quality by an 8-lens multi-agent review; every
major/minor candidate was adversarially verified against the actual working-tree code (19
confirmed, 1 refuted, 0 undecided). Nits skipped verification and are marked as such.

Gates at review time: 1800 unit tests OK, PHPStan level 8 clean.

**26 findings: 1 major (FIXED 2026-08-08), 17 minor (2 FIXED, 15 OPEN), 8 nit (OPEN).** (Two nits
were added during the adversarial verification of the major's fix; the two test-coverage minors
were fixed while restoring the CI coverage gate.)

## Major

- [ordered-recreate-deferral-wedges-watchdog-notified-latch](ordered-recreate-deferral-wedges-watchdog-notified-latch.md)
  — **FIXED (2026-08-08)** — the disconnect-collision deferral never cleared the watchdog
  `notified` latch; a watchdog-triggered recreate colliding with a disconnect stalled the ordered
  consumer / KV / OS watch permanently and silently after reconnect. The deferral now clears the
  latch; pinned by `testWatchdogRecreateDeferredByReconnectRefiresOnceOpenAgain`.

## Minor — behavior

- [ws-control-answer-cap-drops-newest-pong-and-close-echo](ws-control-answer-cap-drops-newest-pong-and-close-echo.md)
  — control-answer cap fires with zero backpressure and drops the NEWEST answers, including the
  RFC 6455 5.5.1 required Close echo (empirically reproduced).
- [draining-publish-bound-exceeds-drain-budget](draining-publish-bound-exceeds-drain-budget.md)
  — Draining-branch publishes each get a fresh full requestTimeoutMs; drain() can overshoot its
  documented budget by K × 10 s.
- [heartbeat-timer-emiterror-before-recovery-unguarded](heartbeat-timer-emiterror-before-recovery-unguarded.md)
  — a throwing PSR-3 logger skips recoverConnection() on the one-shot violation path and kills the
  event loop (deviates from the repo's own #150 pattern).
- [drain-final-close-can-strand-draining](drain-final-close-can-strand-draining.md)
  — drain()'s teardown awaits transport->close() unguarded; a throwing custom transport strands
  Draining despite the new "always reaches Closed" contract.
- [adr9-mismatch-only-greater-masks-consumer-replacement](adr9-mismatch-only-greater-masks-consumer-replacement.md)
  — gap check only fires on `>`; a server-side consumer replacement resets cseq and masks real
  gaps until the old high-water mark is passed (nats.go fires on any inequality).
- [stop-racing-failed-recreate-spurious-terminal-error](stop-racing-failed-recreate-spurious-terminal-error.md)
  — a stop racing a recreate whose attempts exhaust emits a terminal "recreate failed" error for a
  deliberately stopped consumer.
- [kv-mirror-prefixes-applied-before-create-never-reset](kv-mirror-prefixes-applied-before-create-never-reset.md)
  — mirror prefixes applied before createStream() succeeds and never reset; a failed mirror create
  poisons the handle (write misdirection + read blindness).
- [kv-prefix-idempotence-bucket-alias-wrong-bucket](kv-prefix-idempotence-bucket-alias-wrong-bucket.md)
  — KV_ idempotence guard on the `bucket` alias resolves a bucket named "KV_x" to bucket "x"'s
  stream (baseline handled this correctly).
- [ordered-stop-registry-leaks-on-plain-unsubscribe](ordered-stop-registry-leaks-on-plain-unsubscribe.md)
  — the documented legacy `unsubscribe($sid)` stop path strands one orderedStops entry per watch,
  forever.
- [os-addlink-deleted-tombstone-guard-deviates-natsgo](os-addlink-deleted-tombstone-guard-deviates-natsgo.md)
  — addLink over a deleted object's tombstone succeeds here, rejected by nats.go; parity claim in
  docs is wrong. (The live-LINK re-pointing allowance was checked and IS correct parity.)
- [os-watch-exact-name-wildcard-chars-rejected](os-watch-exact-name-wildcard-chars-rejected.md)
  — watch() rejects exact names containing `*`/`>` although such names are legal and encode to
  valid filters; the error message is self-contradictory.
- [service-reply-failure-double-counts-errors](service-reply-failure-double-counts-errors.md)
  — reply-publish failure catch double-counts endpoint errors; $SRV.STATS can report
  num_errors > num_requests (empirically reproduced).

## Minor — docs / disclosure

- [pull-503-signal-rearm-disclosure-mismatch](pull-503-signal-rearm-disclosure-mismatch.md)
  — CHANGELOG + comments say "re-armed by the next delivery"; the (intended) code also re-arms on
  any routine non-503 retire.
- [readme-kv-watch-undefined-js-variable](readme-kv-watch-undefined-js-variable.md)
  — README KV watch example calls `$js->stopOrderedConsumer()` without defining `$js`; copy-paste
  fatals.

## Minor — test quality / coverage

- [test-stop-during-recreate-wall-clock-race](test-stop-during-recreate-wall-clock-race.md)
  — the stop-vs-recreate race test sequences the stop with a 50 ms wall-clock delay; can flake red
  under CI load.
- [test-drain-dead-socket-write-contract-untested](test-drain-dead-socket-write-contract-untested.md)
  — **FIXED (2026-08-08)** — the disclosed "drain() no longer throws on dead-socket write failure"
  contract is now pinned by five DrainFlushBoundedWriteTest cases.
- [test-heartbeat-protocol-violation-recovery-untested](test-heartbeat-protocol-violation-recovery-untested.md)
  — **FIXED (2026-08-08)** — FakeTransport gained a one-shot readLine ProtocolException mode; the
  recovery and forced-Closed arms are pinned in NatsConnectionTest.

## Nits (not adversarially verified)

- [ws-negotiate-rejects-legal-server-max-window-bits](ws-negotiate-rejects-legal-server-max-window-bits.md)
  — handshake fails on a spec-legal `server_max_window_bits` response param the codec decodes for
  free.
- [os-assert-order-early-return-skips-later-chunks](os-assert-order-early-return-skips-later-chunks.md)
  — one unreported ack seq disables order verification for all remaining chunks (`return` vs
  `continue`).
- [os-list-fallback-name-roundtrip-lossy](os-list-fallback-name-roundtrip-lossy.md)
  — list()'s 503 fallback re-encodes names with padding; unpadded foreign tokens silently dropped.
- [ws-codec-unmask-production-dead](ws-codec-unmask-production-dead.md)
  — `WebSocketFrameCodec::unmask()` has no production callers left.
- [test-heartbeat-gap-episode-rearm-unpinned](test-heartbeat-gap-episode-rearm-unpinned.md)
  — the ADR-9 gap signal's de-dup + delivery re-arm semantics have no pinning test.
- [test-repeated-close-assertion-vacuous](test-repeated-close-assertion-vacuous.md)
  — the repeated-close "no second frame write" assertion cannot fail against the double.
- [deferral-stale-candidate-adoption-suppresses-watchdog](deferral-stale-candidate-adoption-suppresses-watchdog.md)
  — a surviving old consumer's frames touch() before the name filter, delaying post-deferral
  recovery until the first idle heartbeat (self-healing, no loss).
- [deferral-condition-sampled-after-reap-awaits](deferral-condition-sampled-after-reap-awaits.md)
  — a reconnect completing during the orphan-reap awaits skips the deferral (loud terminal
  teardown, baseline behavior).

## Refuted during verification (not filed)

- `addlink-live-link-overwrite-contradicts-claimed-natsgo-parity` — the verifier confirmed
  nats.go DOES permit re-pointing an existing link, so the round-1 allowance is correct parity
  (only the deleted-tombstone case diverges, filed above).
