# drain()'s final teardown awaits transport->close() unguarded — a throwing close() strands the connection in Draining, contradicting the new "always reaches Closed" contract

- **Status:** OPEN (filed 2026-08-08, second-round review; adversarially verified)
- **Severity:** minor
- **Type:** contract violation (custom-transport edge) / blocked flow
- **Area:** connection drain (round-1 fix follow-up)
- **Where:** `src/Connection/NatsConnection.php:933-936` (unguarded close in teardown),
  `:755-762` (`closeTransportBestEffort`, used by every other terminal path)

## Problem

drain()'s teardown runs:

```php
$this->releaseRuntimeState();
$this->transport->close()->await();      // unguarded
$this->state = ConnectionState::Closed;
```

`TransportInterface::close()` documents no no-throw guarantee, and every other terminal path uses
`closeTransportBestEffort()` ("already closed/broken sockets must not mask the original failure").
The CHANGELOG promises unconditionally that "any drain write failure falls through to teardown, so
drain() always reaches Closed" and that drain() "no longer THROWS on a dead-socket write failure".
The new dead-socket path deliberately routes into this `close()` on a known-broken socket — if a
custom transport's `close()` propagates the pending-write failure, drain() rethrows with state
stranded in **Draining** forever (`connect()` refuses while Draining; `drain()` requires Open), the
exact outcome the contract change says no longer happens.

In-tree transports happen not to throw, so impact is limited to custom `TransportInterface`
implementations — hence minor.

## Failure scenario

Custom transport whose `close()` errors on an already-broken socket: drain() on a dead socket
emits the write error, falls through to teardown, then `close()` throws — drain() rethrows after
`releaseRuntimeState()` with state stuck in Draining (subsequent publishes route into the bounded
Draining write path against a dead transport; no reconnect possible).

## Suggested fix

Use `$this->closeTransportBestEffort()` in drain()'s teardown (or set `state = Closed` before
awaiting `close()`), matching every other terminal path.
