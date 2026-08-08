# Caller-owned push consumers: no ADR-9 Nats-Last-Consumer gap detection, and heartbeats are withheld from the handler so callers cannot do it themselves

- **Status:** FIXED (2026-08-08) — the shared `callerOwnedPushDispatch()` wrapper tracks the
  delivered consumer sequence and compares each idle heartbeat's `Nats-Last-Consumer` against it,
  surfacing a consumer-sequence-mismatch error (listener + logger) once per gap episode (nats.go
  ErrConsumerSequenceMismatch parity); delivery progress re-arms the signal. Confirmed by
  `testPushConsumerHeartbeatSequenceMismatchIsSurfaced` (fails pre-fix).
  **Remediation after adversarial verification:** the check now arms only once a delivery fixes a
  SESSION baseline (nats.go empty-cmeta parity) — re-attaching to a durable with delivery history
  no longer false-alarms on its first heartbeat; pinned by
  `testPushConsumerHeartbeatMismatchNotSignaledBeforeFirstSessionDelivery`.
- **Severity:** minor (permanent silent loss only in ack-none / max_deliver=1 configurations)
- **Type:** completeness (ADR-9) / silent message loss
- **Area:** JetStream push consumers
- **Where:** `src/JetStream/JetStreamContext.php:1109-1129` (`subscribePushConsumer`),
  `:1153-1188` (`subscribeEphemeralPushConsumer`); `heartbeatLastConsumerSeq()` (`:2940-2945`)
  is consulted only on the ordered path (`:1455-1458`); `handlePushControlMessage()`
  (`:2713-2762`) intercepts every status frame before the user handler

## Problem

Idle-heartbeat status frames carry `Nats-Last-Consumer` (ADR-9), which reveals a delivery gap:
if the heartbeat's last-consumer sequence is ahead of what the client delivered, messages were
missed. The ordered-consumer path checks this; caller-owned push consumers do not, and because
`handlePushControlMessage()` swallows all status-100 frames before the user handler, the caller
cannot implement the check either — the header is never visible.

## Failure scenario

A push consumer with `idle_heartbeat` and `ack_policy: none` (or `max_deliver: 1`) misses
deliveries — an interest gap around a resubscribe window, or local slow-consumer drops. The
heartbeats keep flowing, so the #113 total-silence watchdog never fires. nats.go surfaces this as
`ErrConsumerSequenceMismatch`; here there is no signal on any channel, and with no redelivery
(ack none / max_deliver 1) the gap is permanent.

## Suggested fix

Track the delivered consumer sequence per push subscription (the `$JS.ACK` reply subject already
carries it) and compare against `Nats-Last-Consumer` on each heartbeat; on mismatch emit a
client error (see `jetstream-client-errors-bypass-logger.md`) or invoke an optional
`onSequenceMismatch` callback, mirroring nats.go.
