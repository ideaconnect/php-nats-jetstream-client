# testStopOrderedConsumerDuringInFlightRecreateDoesNotResurrect sequences the stop with a 50 ms wall-clock delay — can flake red under CI load

- **Status:** FIXED (2026-08-08) — the stop is now triggered from the transport onWrite hook when attempt #1's
  CONSUMER.CREATE is observed (EventLoop::queue), replacing the 50ms wall-clock delay; the 5s
  outer deadline remains. The rework was itself validated by an intermediate red (an off-by-one
  landed the stop in attempt #2) and is stable across repeat runs.
- **Severity:** minor
- **Type:** test quality (flaky-by-construction; violates repo timing convention)
- **Area:** tests — ordered-consumer stop race
- **Where:** `tests/Unit/JetStreamContextTest.php:4286` (`EventLoop::delay(0.05, ...)` scheduled
  stop)

## Problem

The test requires the stop to land INSIDE attempt #1's parked create await ("the stop must land
before attempt #2"), asserting `UNSUB 2` appears before the third `CONSUMER.CREATE`. But the stop
is sequenced purely by wall time: `EventLoop::delay(0.05, ...)`. The gap frames `msg1`/`bad3` are
dispatched across separate `processIncoming` loop turns (FakeTransport returns one chunk per
readLine, the connection performs one readLine per turn), with multiple fiber-suspension points
between the timer's scheduling and `bad3`'s dispatch — and Revolt runs due timers at each tick.
Nothing pins the timer behind the dispatch.

If the process stalls ≥50 ms in that window (CI scheduler preemption, GC), the timer fires first:
`stopOrderedConsumer()` unsubscribes sid 2 and deletes ORD1 **before any recreate starts**, `bad3`
then routes to a removed sid, no recreate ever runs, the pump loop spins its full 5 s deadline,
and `assertIsInt($thirdCreate)` fails — a spurious red on a correct fix. The repo convention
(test-timing memory, #70) prefers behavior/wire-event sequencing over wall-clock steps.

## Suggested fix

Trigger the stop from the transport's `onWrite` hook when attempt #1's second
`$JS.API.CONSUMER.CREATE.EVENTS` request is observed (`EventLoop::queue` the stop from there),
instead of the 50 ms delay; keep the 5 s deadline as the outer bound.
