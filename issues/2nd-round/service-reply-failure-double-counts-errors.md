# Reply-publish failure catch double-counts endpoint errors — one request can add 2 to num_errors, so $SRV.STATS can report num_errors > num_requests

- **Status:** FIXED (2026-08-08) — the reply-publish-failure catch gates errors++/request_error on
  $errorHeaders === null (handler succeeded, reply write is the request's sole fault) and always
  records lastError; num_errors can no longer exceed num_requests. Pinned by
  `testHandlerErrorWithFailedErrorReplyCountsSingleError` (pre-fix red: num_errors 2) plus the
  preserved handler-success arm.
- **Severity:** minor
- **Type:** ADR-32 stats correctness / observer-event ordering
- **Area:** micro services (round-1 fix follow-up)
- **Where:** `src/Services/Service.php:282` and `:299` (handler-error increments), `:358-372`
  (new reply-failure catch incrementing again), `:316-323` (terminal `request_end` emitted before
  the reply publish)

## Problem

The handler-error paths already do `$endpoint->errors++` (ServiceError at `:282`, generic
Throwable at `:299`) before building the error reply. The NEW catch around the reply publish
unconditionally does `$endpoint->errors++; $endpoint->lastError = ...;` plus a second
`request_error` observer event — so a request whose handler errored AND whose error-reply publish
then failed is counted twice. `requests++` happens once per request, so
`num_errors > num_requests` is reachable in the ADR-32 STATS payload. The second `request_error`
also fires **after** the `request_end` the code's own comments call "terminal", and carries the
hardcoded code `HANDLER_ERROR`, contradicting the original ServiceError's custom code.

Empirical repro (handler disconnects AND throws): `num_requests=1, num_errors=2`.

## Failure scenario

Handler throws ServiceError (errors=1, `request_error` emitted); the connection drops before the
reply publish; `publishResponse()->await()` throws; the new catch runs: errors=2 for a single
request, plus a late second `request_error`. An operator computing error rate from `$SRV.STATS`
sees >100% errors.

## Suggested fix

In the new catch, only increment errors / emit `request_error` when `$errorHeaders === null`
(i.e. the handler itself succeeded and the failure is purely the reply write); otherwise just
record `lastError`, so a single request counts one error.
