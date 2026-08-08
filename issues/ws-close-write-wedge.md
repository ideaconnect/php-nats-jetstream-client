# WebSocketTransport::close() can suspend forever on the Close-frame write, permanently wedging recovery

- **Status:** FIXED (2026-08-07) — the Close-frame write now runs in its own fiber awaited with a
  0.25 s bound (`CLOSE_FRAME_WRITE_TIMEOUT`); on timeout the socket close proceeds and errors the
  wedged write out. `readLine()` additionally pins its socket in a local so a reader resuming inside
  the bounded close window cannot deref the nulled property (adjacent pre-existing race surfaced by
  a 3-lens adversarial verification, all lenses SOUND). Regression-pinned by
  `WebSocketTransportTest::testCloseCompletesWhenCloseFrameWriteWedgesOnBackpressure` (fails against
  the pre-fix code with a bounded CancelledException) plus healthy-path and no-op-close tests, using
  the new `tests/Support/WedgedWriteSocket.php` double that mirrors Amp's suspend-on-write /
  error-on-close semantics. Note: the fixed close() also converts the sibling PONG/Close-echo
  inline-write stalls (see `ws-unguarded-pong-write-drops-frames.md`) from unrecoverable deadlocks
  into stalls a user disconnect()/drain() can now break.
- **Severity:** critical (WebSocket transport only; `AmpSocketTransport` is immune)
- **Type:** blocked flow / deadlock
- **Area:** Transport / WebSocket
- **Where:** `src/Transport/WebSocketTransport.php:356-362`

## Problem

`close()` writes a best-effort Close frame *before* calling `$socket->close()`:

```php
try {
    $socket->write(WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_CLOSE, ''));
} catch (\Throwable) {
    // ...
}
$socket->close();
```

The `try/catch` only guards a *throw*. Amp's `WritableResourceStream::write()` does not throw
when the write buffer is full — it queues the bytes and **suspends the calling fiber** until the
buffer drains. Suspension is not an exception, so the guard never engages and `$socket->close()`
is never reached.

## Failure scenario

1. The peer stalls reading (e.g. zero TCP receive window) while the client keeps publishing; the
   OS send buffer and Amp's write queue fill, so publisher fibers are suspended inside
   `socket->write()`.
2. The heartbeat watchdog trips `maxPingsOut` → `recoverFromHeartbeatFailure()` →
   `recoverConnection()` → `transport->close()->await()`.
3. `close()` pushes the Close frame onto the already-full write queue and **suspends**. The
   socket is never closed, so the queued writes are never errored out.
4. `$this->socket` was already nulled at the top of `close()`, so no other caller can reach the
   socket either; the ping timer self-cancels because state left `Open`. Nothing remains that
   can break the deadlock — the whole client is wedged permanently.

The same wedge applies to the `__destruct` path (`NatsConnection::__destruct` queues
`transport->close()`).

## Expected behavior

`close()` must always reach `$socket->close()`. The Close-frame courtesy write must be
non-blocking or time-bounded (RFC 6455 allows simply dropping the TCP connection; the Close
handshake is best-effort for a client).

## Suggested fix

Close the raw socket first when the write buffer is non-empty, or bound the Close-frame write
with a short timeout (e.g. race it against a `TimeoutCancellation`/`EventLoop::delay` and call
`$socket->close()` regardless), mirroring how `AmpSocketTransport::close()` never writes.
