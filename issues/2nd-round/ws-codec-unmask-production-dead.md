# WebSocketFrameCodec::unmask() is production-dead after the masked-frame rejection — kept alive only by its own unit test

- **Status:** RESOLVED (2026-08-08) — kept as `@deprecated` rather than removed: `unmask()` has been public
  since 2.7.x, so removal would be an undisclosed bc-break; the docblock now points harnesses at
  `decode(..., allowMasked: true)`. Still behavior-pinned by
  `testDeprecatedUnmaskStillInvertsEncodeMasking`.
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
