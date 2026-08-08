# Infinite pipelined consume treats a transient 503 as terminal — the worker stops permanently, silently when no onError is set

- **Status:** FIXED (2026-08-07) — the infinite engine now classifies 503 as routine-with-backoff
  (nats.go Consume parity; finite/fetch semantics unchanged). The first 503 of a streak fires
  `onError` once as an operator signal, re-armed by the next delivery. Confirmed by
  `PullPipelineTest::testInfiniteRunPollsThroughTransientNoResponders` and
  `testInfiniteRunWithoutOnErrorSurvivesTransientNoResponders` (both fail pre-fix), plus a live
  integration pass (JetStream + multi-consumer suites, 54 tests).
  **Remediation after adversarial verification:** the one-shot signal now re-arms on any routine
  non-503 retire (a 404/408 proves the JS API answers again), so a later outage on an idle stream
  is reported too — pinned by `testNoRespondersSignalReArmsAfterRoutineNonEmptyGap`.
- **Severity:** major
- **Type:** blocked flow (silent permanent stop) / spec correctness
- **Area:** JetStream pull pipelining engine
- **Where:** `src/JetStream/JetStreamContext.php:2334-2390` (routine-status classification at
  `:2357-2360`, terminal branch `:2383-2390`)

## Problem

The engine classifies an empty pull retire as routine only for:

```php
$routine = $code === null
    || $code === 404
    || $code === 408
    || ($code === 409 && PullConsumerIterator::isNonTerminalPullStatus($description));
```

A **503** — the no-responders status the server synthesizes when `$JS.API` momentarily has no
responder (CONNECT sets `no_responders: true`, `src/Protocol/ProtocolCodec.php:44`) — falls
through to the terminal branch: `$terminated = true`, the run breaks. With no `onError`
configured, `handle()`'s Future **resolves normally with the processed count**, indistinguishable
from a clean drain.

## Failure scenario

1. Server restarts → client reconnects → the engine's epoch reset re-issues pulls immediately.
2. JetStream on the restarted server is not yet serving `$JS.API.>` → the pull's reply inbox
   receives a 503 status frame.
3. The infinite consumer terminates permanently. Messages accumulate in the stream; nothing is
   consumed again; no exception, no log, no error unless `onError` was configured.

Inconsistencies within the same codebase:
- The sibling transient "409 Leadership Change" **is** treated as routine.
- `publishWithRetry()` retries 503s per ADR-21 (`JetStreamContext.php:1729-1746`).
- The engine is the one continuous path where a transient 503 is terminal. (For single-shot
  `fetchBatch()` a thrown 503 is correct.)

## Suggested fix

Treat 503 as routine-with-backoff in the infinite engine (bounded: e.g. keep retrying with the
existing generation backoff, optionally escalating to `onError` after N consecutive 503s), the
same way nats.go `Consume()` keeps pulling through temporary "no responders" windows. At minimum,
a terminal stop without `onError` must not resolve `handle()` as if it drained cleanly — it
should reject or log loudly (see also `jetstream-client-errors-bypass-logger.md`).
