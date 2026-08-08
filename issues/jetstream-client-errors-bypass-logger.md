# emitClientError() bypasses the PSR-3 logger — a terminally dead ordered consumer is completely silent without an errorListener

- **Status:** FIXED (2026-08-08) — `emitClientError()` now routes through
  `NatsClient::emitError()` (PSR-3 logger + error listener), so a terminally dead ordered consumer
  produces a log line even with no listener configured. Confirmed by
  `testTerminalRecreateFailureReachesTheLogger` (fails pre-fix).
- **Severity:** minor (signal gap that upgrades other failures to "silent")
- **Type:** observability / blocked flow with zero signal
- **Area:** JetStream error surfacing
- **Where:** `src/JetStream/JetStreamContext.php:2770-2782`; contrast
  `NatsConnection::emitError()` (`src/Connection/NatsConnection.php:3557-3574`), which always
  logs via PSR-3 before invoking the listener

## Problem

`JetStreamContext::emitClientError()` invokes only `options()->errorListener` (default null) and
returns without touching the configured logger. Sites that surface *terminal* conditions through
it include:

- ordered recreate exhausting `ORDERED_RECREATE_ATTEMPTS` — both inboxes unsubscribed, watchdog
  cancelled, delivery stops for good (`:1383-1392`);
- ack-metadata parse failures and pull-engine error paths (`:1474-1481`, `:2806-2814`,
  `:2838-2848`).

An application configured with a PSR-3 logger but no listener observes an ordered consumer that
just stops delivering forever, with no log line and no exception anywhere.

## Suggested fix

Mirror `NatsConnection::emitError()`: log through the client's logger (error level for terminal
conditions) before invoking the listener. The logger is reachable via the connection/options; if
not currently exposed, route these through `NatsClient::emitError()` which already does both.
