# WebSocketFrameCodec::unmask() is production-dead after the masked-frame rejection — kept alive only by its own unit test

- **Status:** OPEN (filed 2026-08-08, second-round review; nit — verified by grep: only caller is
  `tests/Unit/WebSocketFrameCodecTest.php:515`)
- **Severity:** nit
- **Type:** dead code
- **Area:** WebSocket frame codec (round-1 fix follow-up)
- **Where:** `src/Transport/WebSocketFrameCodec.php:314`

## Problem

Its only production caller — `WebSocketTransport::consumeSpanningFrame` — now throws on masked
server frames instead of unmasking, and `decode()` applies masking inline (under `allowMasked`).
The sole remaining caller is the codec's own unit test asserting `unmask()` inverts `encode()`'s
masking. The public helper accretes, and its docblock is the only thing keeping it looking alive.

## Suggested fix

Remove `unmask()` and its test (or mark it `@internal` for the test-harness audience the new
`allowMasked` decode parameter already serves).
