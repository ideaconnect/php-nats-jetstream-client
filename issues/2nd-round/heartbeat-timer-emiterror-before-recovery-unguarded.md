# Heartbeat-tick protocol-violation path calls unguarded emitError() before recoverConnection() — a throwing logger skips the recovery and kills the event loop

- **Status:** OPEN (filed 2026-08-08, second-round review; adversarially verified)
- **Severity:** minor
- **Type:** correctness / deviation from the repo's own #150 containment pattern
- **Area:** connection heartbeat / WS one-shot terminal recovery (round-1 fix follow-up)
- **Where:** `src/Connection/NatsConnection.php:3567-3576` (new protocol-violation branch),
  `:3687` (emitError logs before its listener guard), `:973-979` (`emitErrorSafely` docblock
  naming this exact hazard)

## Problem

The new branch in `consumeHeartbeatResponse` runs:

```php
if ($protocolViolation !== null) {
    $this->emitError($protocolViolation);          // unguarded
    try { $this->recoverConnection(); } catch (\Throwable) { $this->state = ConnectionState::Closed; }
    return;
}
```

`emitError()` calls `$this->logger->log(...)` **before** its listener try/catch, so a throwing
PSR-3 logger escapes `emitError` — skipping `recoverConnection()` entirely — and unwinds through
`pingTimerTick` into the `EventLoop::repeat` closure. No `EventLoop::setErrorHandler` exists in
src/, so the throw reaches Revolt's uncaught-throwable path and kills `EventLoop::run()`. The
project's own `emitErrorSafely()` (#150) exists for precisely this and is used on the comparable
containment paths; this new branch deviates.

Worse, the violation is one-shot: the WS transport nulls `pendingTerminal` before throwing, so the
skipped recovery is never retried from a re-raised violation — the connection stays Open on a
corrupt stream with the violation consumed, which is the exact condition the round-1 fix
(`issues/ws-frame-strictness-gaps.md`) claims to close.

## Failure scenario

User configures a PSR-3 logger that can throw (disk full, remote sink down). A one-shot WS
protocol violation (inflate failure / RSV1 gate) surfaces on the heartbeat timer's read →
`emitError`'s logger call throws → `recoverConnection()` never runs and the exception escapes the
timer callback, killing the event loop.

## Suggested fix

Use `$this->emitErrorSafely($protocolViolation)` (or emit after the recovery attempt) in the
protocol-violation branch, matching the #150 containment pattern.
