# Transport writes accept no cancellation/timeout — drain() and flush() bounded-time contracts are unenforceable under write backpressure

- **Status:** FIXED (2026-08-07) — bounded at the connection layer (no `TransportInterface` change,
  so no BC break for custom transports): `NatsConnection::writeBounded()` runs the write on its own
  fiber and bounds only the WAIT; drain()'s UNSUB/PING writes are budgeted by the drain deadline
  (any write failure now falls through to teardown, so drain always reaches Closed — #150), and
  flush()'s PING write is budgeted by the request timeout (wedge → `TimeoutException`). Confirmed
  by `DrainFlushBoundedWriteTest::testDrainCompletesWithinBudgetWhenWritesWedge` and
  `testFlushTimesOutWhenPingWriteWedges` (both hang/fail pre-fix), using the new
  `tests/Support/WedgedWriteTransport.php` double.
- **Severity:** major
- **Type:** blocked flow
- **Area:** Transport contract / Connection
- **Where:**
  - `src/Transport/TransportInterface.php` — `write(string $bytes): Future` (no `Cancellation` parameter)
  - `src/Transport/AmpSocketTransport.php:144-160`, `src/Transport/WebSocketTransport.php:214-236`
  - `src/Connection/NatsConnection.php:776-794` (drain: per-sid UNSUB writes + flush PING write, plain `->await()`)
  - `src/Connection/NatsConnection.php:1377-1386` (`flush()` PING write, plain `->await()`)

## Problem

`readLine()` takes a `Cancellation`, so every read in the client is time-bounded. `write()` does
not — Amp's `WritableResourceStream::write()` suspends the calling fiber when the send buffer is
full and there is no way to bound that suspension from the caller.

For normal `Open`-state operation this is eventually rescued: the heartbeat watchdog trips
`maxPingsOut` and recovery closes the socket, which errors out all pending writes. But `drain()`
**cancels the ping timer first** (`NatsConnection.php:768`) and then performs its UNSUB and PING
writes; a Draining-state read failure is deliberately not escalated to recovery. If the peer has
stalled with a full send buffer:

1. `drain()` suspends forever inside the first UNSUB (or the flush PING) write.
2. The documented drain budget (`$drainDeadline`, `NatsConnection.php:774`) bounds only the
   *read* phases — it is never consulted around the writes.
3. The connection is stuck in `Draining` with the socket open; the documented
   "total drain time cannot exceed ~requestTimeoutMs" contract (#149) is silently violated.

`flush()`/`rtt()` have the same exposure for their PING write (their read loop is bounded, the
write is not), though there the still-armed heartbeat can eventually rescue them.

## Suggested fix

Add an optional `?Cancellation $cancellation = null` to `TransportInterface::write()` (BC:
optional parameter, third-party transports flagged in CHANGELOG), thread the drain deadline /
request timeout through the drain and flush write paths, and treat a cancelled write like a
write failure (recover or fail loudly). Alternatively, bound drain's writes by racing them
against the drain deadline and forcing `transport->close()` when exceeded.
