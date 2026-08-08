# Control-answer cap drops the NEWEST answers — including the RFC 6455 5.5.1 required Close echo — and fires with zero backpressure on a healthy socket

- **Status:** OPEN (filed 2026-08-08, second-round review; adversarially verified, empirically
  reproduced with a 17-ping + Close read batch)
- **Severity:** minor
- **Type:** spec correctness (RFC 6455 5.5.1 MUST / 5.5.3 inversion) / partial regression of #161
- **Area:** WebSocket transport
- **Where:** `src/Transport/WebSocketTransport.php:531-544` (`answerControlFrame` cap), `:585`
  (Close echo through the same capped path)

## Problem

`answerControlFrame()` counts queued-but-not-yet-started answer fibers:

```php
if ($this->pendingControlAnswers >= self::MAX_PENDING_CONTROL_ANSWERS) { return; }
$this->pendingControlAnswers++;
async(function () ... finally { $this->pendingControlAnswers--; });
```

Amp `async()` fibers only start when the read fiber suspends, so during one `processFrames()`
batch the counter monotonically increments and never decrements — the cap fires with **zero actual
backpressure**. Two consequences:

1. The drop policy is the inverse of the RFC 6455 5.5.3 allowance the code's own comment cites:
   the 16 **oldest** ping answers are kept and the **newest** dropped, while 5.5.3 permits eliding
   only older answers in favor of the most recent ping.
2. The OP_CLOSE echo goes through the same capped path (`:585`), so a Close preceded by 16+
   coalesced pings gets **no Close echo at all** — RFC 6455 5.5.1 makes the echo a MUST, and
   baseline wrote it inline unconditionally (#161).

Empirically confirmed: one read chunk of 17 unmasked PINGs (p0..p16) + Close(1000) on an
instant-write socket yields 16 pongs for p0..p15, the pong for p16 dropped, and 0 Close frames.

Mitigation that keeps this minor: in the integrated client the read loop's recovery calls
`transport->close()`, which writes a best-effort empty OP_CLOSE before closing, so the peer
usually still receives *a* Close frame (without the status echo); and nats-server does not burst
WS pings, so the trigger requires a hostile/pathological peer or real backpressure.

## Failure scenario

A peer coalesces 16+ pings ahead of its latest liveness-probing ping (or ahead of a Close) into
one TCP segment. On a fully healthy write path the client answers only the 16 stale pings: a peer
correlating the pong payload of its latest ping marks the client dead; a peer sending Close after
a ping burst never receives the mandatory status-echoing Close. Under real backpressure the same
logic silently drops the Close echo once 16 pongs are parked.

## Suggested fix

Track answers as data, not fibers: one latest-pong payload slot (RFC 6455 5.5.3 semantics —
replace a queued-but-unsent pong payload with the newest instead of dropping the newest) plus a
dedicated slot for the single OP_CLOSE echo so it always goes out, flushed by one writer fiber.
At minimum, exempt OP_CLOSE from the cap (at most one echo is ever sent per connection, so it
cannot contribute to unbounded growth).
