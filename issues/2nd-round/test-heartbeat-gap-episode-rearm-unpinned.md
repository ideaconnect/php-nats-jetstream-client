# The ADR-9 gap signal's "once per gap episode" semantics (de-dup + delivery re-arm) are not pinned by any test

- **Status:** OPEN (filed 2026-08-08, second-round review; nit — not adversarially verified)
- **Severity:** nit
- **Type:** test coverage gap
- **Area:** tests — JetStream push-consumer ADR-9 gap detection (round-1 fix follow-up)
- **Where:** production `src/JetStream/JetStreamContext.php:1193-1200` (`$gapSignaled = true`
  de-dup) and `:1221` (delivery re-arm); test
  `tests/Unit/JetStreamContextTest.php:4131` sends exactly ONE gap heartbeat

## Problem

`testPushConsumerHeartbeatSequenceMismatchIsSurfaced` sends a single gap heartbeat, so removing
`$gapSignaled = true` (spam every gap heartbeat of a persistent gap) or removing the delivery
re-arm (a second gap episode after recovery never reported) both leave the test green. Contrast:
the sibling pull-engine 503 signal's re-arm IS pinned
(`PullPipelineTest::testNoRespondersSignalReArmsAfterRoutineNonEmptyGap`).

## Suggested fix

Extend the mismatch test: after the gap heartbeat, send a second gap heartbeat (assert still
exactly 1 mismatch report), then a delivery advancing the sequence, then another gap heartbeat
(assert a 2nd report).
