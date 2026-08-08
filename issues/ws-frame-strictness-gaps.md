# WebSocket frame-level RFC 6455 strictness gaps: fragmented control frames corrupt reassembly; masked server frames and spurious RSV1 accepted

- **Status:** FIXED (2026-08-08) — `decode()` now rejects fragmented/oversized control frames
  (RFC 6455 5.5) and masked server-to-client frames (5.1; an explicit `allowMasked` mode remains
  for decoding CLIENT-written frames in tests/harnesses), and the transport rejects RSV1 without
  negotiated compression (5.2) instead of blind-inflating. All deferred via the #115 terminal
  pattern so same-read data is never discarded. Confirmed by
  `testDecodeRejectsFragmentedOrOversizedControlFrames`,
  `testDecodeRejectsMaskedServerFrameUnlessAllowed`,
  `testReadLineRejectsFragmentedControlFrameInsteadOfCorruptingReassembly` (the corruption pin),
  `testReadLineRejectsMaskedServerFrameOnSpillPath`, and
  `testReadLineRejectsRsv1WithoutNegotiatedCompression` (all fail pre-fix).
  **Post-verification notes:** the masked-frame check in `decode()` fires from header bytes alone
  and always PREEMPTS the spill path, so `testReadLineRejectsMaskedServerFrameOnSpillPath`
  exercises the batch-decode rejection end-to-end while the spill-path throw remains defensive
  depth; a deferred one-shot violation surfacing on the heartbeat timer's read now recovers the
  connection instead of being swallowed. Remaining lenient-accepts (reserved opcodes dropped,
  RSV2/RSV3 unchecked, RSV1 on continuations tolerated) are documented out-of-scope leniency, not
  corruption paths.
- **Severity:** minor (requires a non-compliant peer; one case causes silent data corruption)
- **Type:** spec correctness / data corruption
- **Area:** Transport / WebSocket
- **Where:** `src/Transport/WebSocketFrameCodec.php:90-165` (no FIN/length validation for control
  opcodes; masked frames silently unmasked at `:152-153`), `src/Transport/WebSocketTransport.php:456-461`
  (`OP_PING` ignores `fin`), `:505-507` (RSV1 inflate not gated on negotiation)

## Problems

1. **Fragmented control frames are not rejected (RFC 6455 §5.5: control frames MUST NOT be
   fragmented, payload ≤ 125).** Worst case is silent corruption: mid-fragmented-data-message
   the peer sends `PING(FIN=0, "abc")` then `CONT(FIN=1, "def")`. The client answers the pong,
   then — because `$this->fragmenting` is true — appends `"def"` (the *ping's* continuation) to
   the data message's fragments and delivers a message with foreign bytes spliced in, feeding
   garbage to the NATS parser instead of failing the connection.
2. **Oversized pings are echoed verbatim**, making the client itself emit a §5.5-violating
   control frame (>125-byte pong).
3. **Masked server→client frames MUST fail the connection (§5.1)**; `decode()` /
   `consumeSpanningFrame()` silently unmask and accept them.
4. **RSV1 on a data frame when compression was not negotiated MUST fail the connection (§5.2).**
   `processFrames()` line 507 inflates based on the frame's RSV1 bit alone (not
   `$compressionActive`), so a spurious RSV1 turns a valid payload into a confusing
   `ProtocolException` (or garbage) rather than a clear protocol violation.

## Suggested fix

In `decode()`: for opcodes ≥ 0x8 require `fin === true` and `payloadLength <= 125`, and reject
masked server frames — throw `ProtocolException` (routed through `$pendingTerminal` per
`ws-midbatch-decode-throw-discards-frames.md`). In `processFrames()`: gate inflation on
`$this->compressionActive && $frame['rsv1']` and treat RSV1-without-negotiation as a protocol
violation; truncate or reject oversized ping echoes.
