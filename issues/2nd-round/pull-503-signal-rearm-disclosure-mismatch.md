# CHANGELOG and inline comments say the one-shot 503 signal is "re-armed by the next delivery" — the code (intentionally) also re-arms on any routine non-503 retire

- **Status:** OPEN (filed 2026-08-08, second-round review; adversarially verified)
- **Severity:** minor
- **Type:** documentation / disclosed-contract mismatch (code is correct, text is wrong)
- **Area:** JetStream pull pipelining engine (round-1 fix follow-up)
- **Where:** `CHANGELOG.md:120-121`, `src/JetStream/JetStreamContext.php:2494-2497` and `:2645`
  (delivery-only wording) vs `:2658-2666` (the wider re-arm)

## Problem

Three places state the one-shot 503 `onError` signal is "re-armed by the next delivery": the
CHANGELOG `[Unreleased]` pull-engine bullet and the two inline comments. The implementation
deliberately does more — any routine **non-503** retire (404/408/non-terminal 409/client-side
deadline) also re-arms it (`$noRespondersSignaled = false`, `:2658-2666`), because a 404/408
proves the JS API answers again. That wider re-arm is the *documented remediation* in
`issues/pull-engine-503-terminal-silent-stop.md`, pinned by
`PullPipelineTest::testNoRespondersSignalReArmsAfterRoutineNonEmptyGap` — so the code is the
intended contract and the released-facing text is what's wrong.

## Failure scenario

An operator of an idle pull consumer on a flapping server (503 outage → 404 empty poll → 503
outage …) reads the CHANGELOG contract and expects at most one `onError(503)` until a message is
actually delivered; instead `onError` fires once per outage episode. Conversely, someone relying
on the header comment alone could conclude a second outage on a never-delivering stream goes
unreported and build external monitoring for a gap that does not exist.

## Suggested fix

Amend `CHANGELOG.md:121` and the comments at `JetStreamContext.php:2495` and `:2645` to say the
signal is re-armed by the next delivery **or any routine non-503 retire** (a 404/408/deadline
proves the JS API answers again) — i.e. one `onError` per no-responders episode.
