# A permission-rejected pull-pipeline inbox subscription leaves the engine polling forever with zero deliveries — the #167 fail-fast latch covers only the mux inbox

- **Status:** FIXED (2026-08-08) — generalized the #167 mechanism: a recoverable permissions
  `-ERR` naming a subscription subject now notifies a per-sid rejection handler
  (`NatsConnection::markSubscriptionRejectionHandler()` / `NatsClient::onSubscriptionRejected()`);
  the pull engine registers one and fails `handle()` fast with a clear error naming the
  `_INBOX.JS.PULL.>` wildcard to grant. Confirmed by
  `testPermissionRejectedPullInboxFailsFastInsteadOfSpinning` (spins/crashes pre-fix).
- **Severity:** minor (misconfigured-permissions scenario; but the failure mode is an infinite silent spin)
- **Type:** blocked flow (silent)
- **Area:** JetStream pull pipelining / connection error handling
- **Where:** `src/JetStream/JetStreamContext.php:2140` (`_INBOX.JS.PULL.<nuid>.*` subscribe),
  `:2219-2220`; `-ERR` handling in `src/Connection/NatsConnection.php:3046-3071` — the
  permission-rejection latch (`:3055-3062`, #167) matches only `$muxBase`

## Problem

Issue #167 added a fail-fast path for the muxed *request* inbox: when the server answers the
`SUB <muxBase>.*` with `-ERR 'Permissions Violation for Subscription to …'`, the mux state is
dropped and `request()`/`requestMany()` throw a clear error instead of timing out forever.

The pull-pipelining engine has the same shape — one long-lived wildcard inbox subscription
(`_INBOX.JS.PULL.<nuid>.*`) that every pull reply is routed through — but no equivalent latch:

1. Account permissions deny subscribing to the pull inbox wildcard; the server's async `-ERR` is
   classified recoverable and only emitted to the errorListener (`NatsConnection.php:3064-3068`).
2. Every pull request is still published successfully; replies can never be delivered.
3. Each pull retires via the silent client-side deadline (`terminalCode = null`), which the
   engine classifies as **routine** (`JetStreamContext.php:2357-2360`) → it re-pulls forever.
   `handle()` never returns, `onError` never fires, no exception is thrown — an infinite,
   invisible spin. (`fetchBatch()`/`directGetBatch()`/push deliver inboxes at least surface
   timeouts to the caller, though also without naming the cause.)

## Suggested fix

Generalize the #167 mechanism: when a recoverable `-ERR` permission violation names a
subscription subject, look it up in `subscriptionMeta` and notify the owning layer (e.g. an
internal per-sid rejection callback). The pull engine should then terminate with a clear
"pull reply inbox rejected by server permissions" error through `onError`/`handle()`, mirroring
the mux behavior. The full per-request-inbox fallback design from #167 remains an optional
enhancement.
