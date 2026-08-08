# Third-round review — 2026-08-08

Scope: the uncommitted 2nd-round fix batch (~2,160 insertions across 43 files; baseline = the
pushed `3ddd7c1`). Six read-only review lenses (connection, JetStream state machine, WebSocket
slot redesign, KV/OS, services + cross-cutting quality, test quality) with per-finding
adversarial verification (7 confirmed, 0 refuted, 0 undecided; 3 nits unverified). Overlapping
findings from multiple lenses are merged below.

**6 unique findings — ALL FIXED (2026-08-08): 4 minor (code), 1 minor (test), 1 nit (test).**
Each Status block below names the fix and its pinning test; the three code fixes with a
behavioral contract were verified red against the pre-fix code.

## Minor — code

- [ondefunct-race-resurrects-unstoppable-consumer](ondefunct-race-resurrects-unstoppable-consumer.md)
  — the new onDefunct cleanup never latches `stopped`; racing an in-flight recreate resurrects a
  consumer that can no longer be stopped (regression vs the recoverable pre-batch race).
- [ws-window-bits-bare-form-and-quoted-string](ws-window-bits-bare-form-and-quoted-string.md)
  — bare `server_max_window_bits` accepted on a wrong RFC citation (grammar makes the value
  mandatory), `=08` passes, and the RFC-legal quoted form `"12"` is still rejected.
- [kv-create-reset-wipes-confirmed-mirror-prefixes](kv-create-reset-wipes-confirmed-mirror-prefixes.md)
  — create()'s top-of-closure prefix reset wipes server-CONFIRMED mirror prefixes when a
  re-create fails; bind() in the same diff got the placement right.
- [deferral-rewind-duplicate-deliveries](deferral-rewind-duplicate-deliveries.md)
  — the rewind doesn't snapshot `lastStreamSeq`; a candidate that delivered before the disconnect
  plus a surviving old consumer yields duplicate deliveries (pre-fix this sub-case was
  exactly-once).

## Minor — tests

- [test-drain-budget-wall-clock-headroom](test-drain-budget-wall-clock-headroom.md)
  — the drain-budget pin's 1.4 s ceiling leaves ~0.6 s scheduling headroom; false-red risk under
  CI load.

## Nits

- [test-addlink-message-over-pinned](test-addlink-message-over-pinned.md)
  — full-message assertSame over-pins wording.
