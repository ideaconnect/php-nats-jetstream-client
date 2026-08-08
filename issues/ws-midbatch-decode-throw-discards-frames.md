# WebSocket mid-batch decode/inflate/fragment-bound throws discard data frames already decoded from the same read

- **Status:** FIXED (2026-08-08) — `WebSocketFrameCodec::decode()` reports strictness violations
  via a `$terminal` out-param (valid same-read frames returned first, buffer trimmed only past
  them), and the transport routes the oversize-declaration, fragment-bound, and inflate failure
  paths through the `$pendingTerminal` deferral (#115 pattern). Confirmed by
  `testDecodePayloadLengthOutOfBoundsReportsTerminalAfterValidFrames` and
  `testReadLineReturnsSameReadDataBeforeOversizedLengthViolation` (fail pre-fix).
  **Post-verification note:** a deferred one-shot terminal (inflate/RSV1/fragment-bound) surfacing
  on the heartbeat timer's read is now surfaced + recovered (previously that specific interleaving
  swallowed it); the `decode()` throw→`$terminal` contract change is disclosed in the CHANGELOG.
- **Severity:** minor (requires a misbehaving/hostile server, or a >64 MiB frame)
- **Type:** message loss on error paths
- **Area:** Transport / WebSocket
- **Where:**
  - `src/Transport/WebSocketFrameCodec.php:133-134` (`decode()` length-bound throw — the local
    `$frames` list is discarded and the buffer left untrimmed)
  - `src/Transport/WebSocketTransport.php:556+` (`enforceFragmentBound()` throw)
  - `src/Transport/WebSocketTransport.php:507, 539-541` (`inflate()` throw)

## Problem

The transport goes to great lengths (`$pendingTerminal`, #115) to deliver same-read data before
surfacing a Close frame or an RFC 6455 §5.4 violation. Three terminal paths still throw
mid-batch, before the already-accumulated output is returned:

1. A frame declaring a payload above `MAX_FRAME_PAYLOAD` (64 MiB): `decode()` throws; frames
   already parsed into the local `$frames` list are lost.
2. A continuation pushing fragment reassembly past `maxMessageBytes`: `enforceFragmentBound()`
   throws past `$out`.
3. A corrupt permessage-deflate payload: `inflate()` throws past `$out`.

In each case the caller reconnects and `connect()` resets `$readBuffer`, so valid `MSG` frames
decoded from the same read are lost permanently. Case 1 is also reachable legitimately when a
server with a near-64 MiB `max_payload` coalesces protocol lines into a frame just over the cap.

## Suggested fix

Route these three failure paths through the existing `$pendingTerminal` deferral: return the
data already decoded in this read, then surface the exception on the next `readLine()`. For the
codec throw, trim the consumed prefix and stash parsed frames before throwing (mirroring
`ProtocolParser::takeParsedFrames()`, #147).
