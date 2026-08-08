# server_max_window_bits parsing: the bare value-less form is accepted on a mis-cited RFC 7692 7.1.2.1, "=08" passes despite the no-leading-zero grammar, and the RFC-legal quoted-string form is still rejected

- **Status:** FIXED (2026-08-08) — the parameter now matches
  `/^server_max_window_bits=(?:([89]|1[0-5])|"([89]|1[0-5])")$/i`: the value is required (RFC 7692
  section 7 ABNF), leading zeroes and out-of-range values are rejected, and the RFC 6455 9.1
  quoted-string spelling is accepted. Comment, test docblocks, and the CHANGELOG citation
  corrected. Pinned by `testHandshakeAcceptsServerMaxWindowBitsWithinRange` (token AND quoted
  forms) and `testHandshakeRejectsOutOfRangeWindowBitsAndClientMaxWindowBits` (bare, =7, =16, =08,
  client_max_window_bits).
- **Severity:** minor (bare-form acceptance + mis-citation), nit (quoted-string rejection)
- **Type:** spec correctness / interop / doc accuracy
- **Area:** WebSocket permessage-deflate negotiation (2nd-round fix follow-up)
- **Where:** `src/Transport/WebSocketTransport.php:918-932` (comment, regex, error text);
  the mis-citation is replicated in `testHandshakeAcceptsServerMaxWindowBitsWithinRange`'s
  docblock, the CHANGELOG `[Unreleased]` amendment, and the 2nd-round issue Status

## Problem

RFC 7692 §7's ABNF makes the value MANDATORY for `server_max_window_bits`
(`server-max-window-bits = "server_max_window_bits" "=" window-bits`; only
`client-max-window-bits` has the optional-value form), and §7.1.2.1 defines it as "a decimal
integer value without leading zeroes between 8 to 15, inclusive". The new acceptance regex
`/^server_max_window_bits(?:=([0-9]{1,2}))?$/i` with its `!isset($bits[1])` arm:

1. accepts the value-less bare form — a grammar-invalid response parameter RFC 7692 §5 directs
   the client to fail the connection on — and cites 7.1.2.1 as permitting it (the citation is
   wrong, and it is repeated in the test docblock and CHANGELOG);
2. accepts the leading-zero spelling `=08` via `[0-9]{1,2}` + `(int)` cast;
3. still rejects the quoted-string spelling `server_max_window_bits="12"`, which the
   Sec-WebSocket-Extensions ABNF (RFC 6455 §9.1: `extension-param = token [ "=" (token |
   quoted-string) ]`) explicitly permits as an equivalent encoding.

The wrong bare-form behavior is now PINNED as required by the test ("a bare
server_max_window_bits must be accepted"), so a future correction has to fight a test asserting
the wrong spec reading.

## Failure scenario

A broken peer answers `permessage-deflate; server_no_context_takeover; server_max_window_bits`
(no value): the client proceeds with compression instead of failing per §5. A conformant peer
using the quoted form `server_max_window_bits="12"` still fails the handshake — the error text's
"accepted: ... server_max_window_bits=8..15" claims acceptance of a value it rejects in one of
its two legal encodings.

## Suggested fix

One regex handling both legal encodings and only them:
`/^server_max_window_bits=(?:([89]|1[0-5])|"([89]|1[0-5])")$/` (value required, no leading
zeroes, optional balanced quotes); correct the comment, test docblock, and CHANGELOG citation
(value REQUIRED per §7 ABNF; quoted-string legal per RFC 6455 §9.1); pin bare and `=08` as
rejected and `"12"` as accepted.
