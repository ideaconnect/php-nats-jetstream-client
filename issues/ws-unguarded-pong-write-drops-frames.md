# WebSocket ping-reply write is unguarded — a write failure discards already-decoded MSG frames from the same read

- **Status:** FIXED (2026-08-07) — the pong answer and the RFC 6455 Close echo now go out via
  `answerControlFrame()`: a fire-and-forget fiber with an internal catch, so a write failure can no
  longer discard same-read decoded frames, and a backpressure-suspended answer can no longer park
  the read fiber (the suspension class of the close() wedge). Confirmed by
  `WebSocketTransportTest::testReadLinePreservesSameReadDataWhenPongWriteFails` and
  `testReadLineNotStalledByWedgedPongWrite` (both fail pre-fix), with new `failWrites`/scripted-read
  support in the test doubles.
- **Severity:** major (WebSocket transport only)
- **Type:** message loss
- **Area:** Transport / WebSocket
- **Where:** `src/Transport/WebSocketTransport.php:458-461` (contrast the deliberately guarded Close echo at `:472-476`)

## Problem

In `processFrames()`:

```php
case WebSocketFrameCodec::OP_PING:
    $this->socket?->write(WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_PONG, $frame['payload']));
    break;
```

The pong write has **no try/catch**, unlike the Close echo a few lines below. When it throws,
the exception propagates out of `processFrames()` → `drainDataFrames()` → the `readLine()`
future, and the local `$out` — which already holds the payload of data frames decoded from the
same read — is discarded. Those bytes were consumed from `$readBuffer` by
`WebSocketFrameCodec::decode()`, so they are gone: core NATS does not resend, and the ensuing
`recoverConnection()` replays SUBs, not missed messages.

## Failure scenario

1. Server coalesces `[BINARY "MSG …"][PING]` into one TCP segment, then dies (RST) or the
   socket write side errors moments later.
2. `decode()` consumes both frames; `processFrames()` appends the MSG payload to `$out`, then
   hits `OP_PING` and the write throws `ClosedException`.
3. The exception aborts the read; `$out` is discarded → the MSG is silently lost.

Secondary effect: under backpressure the same unguarded write **suspends the read fiber**
mid-drain, stalling inbound delivery behind an outbound pong (the exact pattern the codebase
avoids elsewhere via `$pendingTerminal`, #115).

## Suggested fix

Wrap the pong write in the same try/catch as the Close echo and defer the failure via
`$pendingTerminal ??=` so the already-decoded data frames in `$out` are returned first, then the
error surfaces on the next `readLine()`. Consider making the pong write non-blocking (queue it)
so a stalled peer cannot suspend the read path.
