# Drain-budget test's 1.4 s wall-clock ceiling leaves only ~0.6 s scheduling headroom — false-red risk under CI load

- **Status:** FIXED (2026-08-08) — the test's budget scales to requestTimeoutMs 3000: the post-fix run takes
  ~1 budget (~3 s), the smallest pre-fix overshoot adds a full extra budget (6 s), and the ceiling
  sits at 4.5 s — >= 1.5 s of scheduling headroom on both sides, instead of the previous ~0.6 s.
- **Severity:** minor
- **Type:** test quality (wall-clock bound too tight)
- **Area:** tests — connection drain budget (2nd-round fix's pinning test)
- **Where:** `tests/Unit/DrainFlushBoundedWriteTest.php:301-306`
  (`testDrainBudgetBoundsSerialHandlerPublishesAcrossBacklog`)

## Problem

The test discriminates the fix by asserting elapsed < 1.4 s where one extra full-requestTimeout
publish (800 ms) would push a pre-fix run past 1.6 s. That leaves ~0.6 s of scheduling headroom
between the legitimate post-fix duration (~0.8 s budget) and the ceiling — the same headroom
class the repo just fixed in the WPI integration test (#70 family). Under a loaded CI runner
(xdebug-instrumented coverage jobs run this suite too) a legitimate run can exceed 1.4 s.

## Suggested fix

Scale the margins proportionally: raise the test's `requestTimeoutMs` (e.g. 800 → 3000) so the
post-fix duration is ~3 s, the pre-fix discrimination point is ~6 s, and the ceiling can sit at
~4.5 s with ≥1.5 s headroom on both sides. Keep the exact delivery-set and error assertions
unchanged.
