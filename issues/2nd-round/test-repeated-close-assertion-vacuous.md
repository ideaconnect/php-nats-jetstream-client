# testCloseWithoutSocketAndRepeatedCloseAreNoOps' "no second frame write" assertion is vacuous — the double rejects post-close writes, so the assertion passes with or without the guard

- **Status:** FIXED (2026-08-08) — ScriptedChunkSocket gained a `writeAttempts` counter incremented before its
  closed check, and the test now asserts the attempt count does not grow across the repeated
  close. Verified: with the `$this->socket = null` release removed, the new assertion fails (2 vs
  1) while the old writtenBytes assertion stayed green — proving the prior vacuousness.
- **Severity:** nit
- **Type:** test quality (assertion cannot fail)
- **Area:** tests — WebSocket transport close
- **Where:** `tests/Unit/WebSocketTransportTest.php:769`;
  `tests/Support/ScriptedChunkSocket.php` (`write()` throws when closed, before recording);
  `src/Transport/WebSocketTransport.php:398` (close swallows frame-write failures)

## Problem

The test snapshots `writtenBytes()`, calls close() again, and asserts the bytes are unchanged —
"a no-op, not a second frame write". But `ScriptedChunkSocket::write()` starts with
`if ($this->closed) { throw new ClosedException(...); }` (appending to `$written` only
afterwards), and `WebSocketTransport::close()` swallows the frame-write failure in its async fiber
(`catch (\Throwable) {}`). If close() regressed to NOT nulling `$this->socket` (the guard under
test), the second frame write would throw, be swallowed, `writtenBytes()` stays equal — and the
assertion still passes.

## Failure scenario

The `$this->socket = null` release in close() is removed; a repeated close re-attempts the
Close-frame write (and re-runs socket close). The test keeps passing because the double silently
absorbs the extra write attempt, so the documented "no second frame write" regression goes
undetected.

## Suggested fix

Add a `writeAttempts` counter to ScriptedChunkSocket (incremented before the closed check, like
`WedgedWriteSocket::writeAttempts`) and assert the attempt count did not grow across the repeated
close.
