# The disclosed heartbeat-timer protocol-violation recovery has no test — and no transport double can even throw ProtocolException from readLine()

- **Status:** FIXED (2026-08-08) — `FakeTransport` gained a one-shot `throwOnNextRead` failure
  mode (modelling the WS codec's one-shot deferred violations), and the branch is pinned by
  `NatsConnectionTest::testHeartbeatReadRecoversFromOneShotProtocolViolation` (error listener
  receives the ProtocolException and a second CONNECT proves recovery) and
  `testHeartbeatProtocolViolationForcesClosedWhenRecoveryUnavailable` (failed recovery marks the
  connection Closed). Added while closing the post-review coverage gap.
- **Severity:** minor
- **Type:** test coverage gap for a disclosed contract change
- **Area:** tests — connection heartbeat / WS one-shot terminal recovery
- **Where:** production branch `src/Connection/NatsConnection.php:3550-3557` (ProtocolException
  catch in the heartbeat tick's self-read) + `:3567-3576` (emitError + recoverConnection); no
  covering test anywhere in the diff or existing suite

## Problem

CHANGELOG discloses: "a one-shot deferred violation surfacing on the heartbeat timer's read now
recovers the connection instead of being silently swallowed" (baseline had only
TransportClosedException + a generic `catch (\Throwable) { return; }` swallow). No test arms the
ping timer against a transport whose `readLine()` throws ProtocolException — and structurally none
*can*: FakeTransport's only exception mode is the EOF sentinel (TransportClosedException), so the
branch is unreachable by the existing harness. The neighboring consumeHeartbeatResponse behaviors
(EOF recovery, no-recovery-without-EOF, recovery-failure-marks-closed, parser-push failure) are
all covered; this new branch is the one exception.

## Failure scenario

The branch regresses to the pre-fix generic swallow (someone reorders the catches or narrows the
ProtocolException catch away). A WebSocket one-shot deferred violation (inflate failure, RSV1
gate, fragment bound) whose offending bytes are already consumed then surfaces on a
heartbeat-timer read, is swallowed forever, and the corrupt stream keeps running silently —
undetected by any test.

## Suggested fix

Add a FakeTransport mode to throw a ProtocolException from `readLine()` once (after connect),
then a unit test with a small ping interval: assert the error listener receives the
ProtocolException and a reconnect (second CONNECT on the wire) or Closed state follows. Note this
test would also have caught the sibling finding
`heartbeat-timer-emiterror-before-recovery-unguarded.md` if paired with a throwing logger.
