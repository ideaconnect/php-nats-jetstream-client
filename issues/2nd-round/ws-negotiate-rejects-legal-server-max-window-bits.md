# negotiateCompression() fails the handshake on a server_max_window_bits response parameter that RFC 7692 explicitly permits and the codec could decode for free

- **Status:** OPEN (filed 2026-08-08, second-round review; nit — not adversarially verified)
- **Severity:** nit
- **Type:** spec interop (conformant-but-unusual peer)
- **Area:** WebSocket permessage-deflate negotiation (round-1 fix follow-up)
- **Where:** `src/Transport/WebSocketTransport.php:872-877` (parameter loop rejects everything
  except the two no_context_takeover tokens)

## Problem

RFC 7692 §7.1.2.1 allows a server to volunteer `server_max_window_bits` in its response — even
when absent from the offer — to limit its own LZ77 window. The client's raw inflater uses the
default 15-bit window (`inflate_init(ZLIB_ENCODING_RAW)`), which decodes any smaller-window
stream correctly, so accepting the parameter costs nothing; the parameter loop instead throws
`ConnectionException` on it. Rejecting an unsupported parameter is itself spec-permitted, and
nats-server's fixed response never includes window bits, so real-target interop is unaffected —
hence nit. (The required `server_no_context_takeover` echo check at `:884-890` is correct per
RFC 7692 §7.1.1.1.)

## Failure scenario

A conformant WebSocket intermediary or future nats-server build answers with
`permessage-deflate; server_no_context_takeover; client_no_context_takeover;
server_max_window_bits=12`; the client throws and the connection fails, even though every frame
from that server would inflate correctly with the existing codec.

## Suggested fix

In the parameter loop, additionally accept `server_max_window_bits[=8..15]` (validate the range,
otherwise reject). Continue rejecting `client_max_window_bits` since it was not offered (RFC 7692
§7.1.2.2 forbids the server sending it unsolicited) and it would change what the client must
deflate.
