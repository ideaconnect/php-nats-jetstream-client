# The disclosed "drain() no longer throws on a dead-socket write failure" contract has no pinning test

- **Status:** OPEN (filed 2026-08-08, second-round review; adversarially verified)
- **Severity:** minor
- **Type:** test coverage gap for a disclosed contract change
- **Area:** tests — connection drain
- **Where:** production path `src/Connection/NatsConnection.php:880-886`
  (`catch (\Throwable $flushError) → emitErrorSafely`); coverage gap in
  `tests/Unit/DrainFlushBoundedWriteTest.php` (only exercises the CancelledException /
  backpressure-wedge path)

## Problem

CHANGELOG `[Unreleased]` discloses: "drain() no longer THROWS on a dead-socket write failure — it
reports it via the error listener and still closes cleanly." Pre-change, the per-sid UNSUB writes
ran outside the try, so a dead-socket write threw out of drain() and stranded state in Draining.
The new outer `catch (\Throwable)` at `:880-886` closes that — but nothing pins it:
`DrainFlushBoundedWriteTest` drives writes that PARK forever (CancelledException path only), and
grep of tests/ shows no test driving a transport whose write **fails** (throws/errors) during
drain. The existing drain tests in `NatsConnectionTest` (unmodified in this diff) all exercise
read-side failures.

## Failure scenario

A future refactor reintroduces the throw (moves the UNSUB loop back outside the try, or rethrows
non-Cancelled write errors): drain() against a dead socket again throws and leaves the connection
stranded in Draining with the socket open — the exact pre-fix bug — and the whole suite stays
green.

## Suggested fix

Add a `DrainFlushBoundedWriteTest` case using a fail-writes-after-N transport mode (write returns
`Future::error(TransportClosedException)` — extend WedgedWriteTransport or FakeTransport's
`throwOnWriteContaining`): assert `drain()->await()` does NOT throw, `state() === Closed`, and the
error listener received the write failure.
