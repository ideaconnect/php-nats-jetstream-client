# WebSocket handshake response validation gaps: Upgrade/Connection headers unchecked; permessage-deflate accepted even when never offered

- **Status:** FIXED (2026-08-08) — the handshake now requires `Upgrade: websocket` and a
  `Connection` token list containing `Upgrade` (RFC 6455 4.1), rejects any extension response when
  none was offered, and validates permessage-deflate parameters against the offered
  `client_no_context_takeover; server_no_context_takeover` (RFC 7692 — this codec inflates per
  message). Confirmed by `testHandshakeRejectsMissingUpgradeHeaders`,
  `testHandshakeRejectsUnsolicitedCompression`, and `testHandshakeValidatesCompressionParameters`
  (fail pre-fix), driven through a real scripted-HTTP loopback handshake.
  **Remediation after adversarial verification:** the server must now also ECHO
  `server_no_context_takeover` when accepting (RFC 7692 7.1.1.1) — accepting without it means
  possible context takeover this per-message-inflate codec cannot decode.
- **Severity:** minor
- **Type:** spec correctness (RFC 6455 §4.1, RFC 7692)
- **Area:** Transport / WebSocket handshake
- **Where:** `src/Transport/WebSocketTransport.php:622-649`

## Problems

The handshake validates only the 101 status line and `Sec-WebSocket-Accept`:

1. **`Upgrade: websocket` / `Connection: Upgrade` are never checked.** RFC 6455 §4.1 requires
   failing the connection when either is missing; a misbehaving proxy answering 101 with a
   plausible `Sec-WebSocket-Accept` but no upgrade headers is accepted and the client proceeds
   to speak WebSocket into a non-WebSocket stream.
2. **Un-offered extension accepted.** With `webSocketCompression = false` the client sends no
   `permessage-deflate` offer, but the response parsing (`:639-644`) sets
   `$compressionActive = true` whenever the response mentions `permessage-deflate` — without
   checking `$this->options->webSocketCompression`. RFC 6455 requires failing the connection on
   an extension that was not offered; instead the client starts RSV1-deflating its CONNECT (the
   server rejects it → confusing connect failure) and inflating inbound frames.
3. **Extension parameters are never parsed.** A server accepting compression without
   `no_context_takeover` semantics compatible with the per-message `inflate_init` in
   `WebSocketFrameCodec::inflate()` is silently accepted; if the server used context takeover,
   every message after the first would fail to inflate or garble.

## Suggested fix

After parsing the response headers: require `Upgrade: websocket` and a `Connection` header
containing `upgrade` (case-insensitive); only honor `permessage-deflate` when it was offered,
and parse the accepted parameters — reject a response that negotiates context takeover the codec
cannot honor (or send `client_no_context_takeover; server_no_context_takeover` in the offer and
require them echoed).
