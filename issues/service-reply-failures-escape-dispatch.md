# Service reply-publish failures escape the endpoint callback into the shared dispatch loop; the schema-validation error reply is remotely triggerable and unguarded

- **Status:** FIXED (2026-08-07) — (a) non-UTF-8 correlation ids are dropped in
  `contextCorrelationId()` (kills the remotely-triggerable JsonException at the source), (b) the
  validation-error reply is wrapped in a best-effort guard, (c) the main reply publish gained a
  `catch (\Throwable)` for connection-level failures (recorded on the endpoint, never escaping),
  (d) `ServiceError` codes are CR/LF-collapsed like descriptions. Confirmed by
  `ServiceTest::testValidationReplyToleratesNonUtf8CorrelationHeader`,
  `testServiceErrorCrLfCodeIsSanitizedAndReplyStillSent`, and
  `testReplyPublishConnectionFailureIsRecordedNotEscaping` (all three error pre-fix).
- **Severity:** major
- **Type:** blocked flow (aborts sibling deliveries) / robustness
- **Area:** Services (NATS micro)
- **Where:** `src/Services/Service.php:242-250` (validation-error reply — fully unguarded),
  `:318-320` (response publish — catches `\JsonException` only), `:652-658`
  (`serviceErrorHeaders()` — `$code` not sanitized)

## Problem

An exception thrown out of a subscription handler aborts the current `drainAllPending()` pass in
`NatsConnection`, delaying sibling subscriptions' queued deliveries and surfacing as a spurious
connection-level error to whichever fiber ran the read — the exact failure mode the code's own
#97 containment comment (Service.php:320-326) says must not happen. Three escapes remain:

1. **Remotely triggerable (A):** with `withSchemaValidator()` configured, a requester sends a
   header `X-Request-Id: <non-UTF-8 bytes>` (header values are raw bytes; nothing validates
   UTF-8) plus a payload failing validation. The validation-error reply at `:242` calls
   `errorPayload(correlationId: <raw bytes>)` → `json_encode(..., JSON_THROW_ON_ERROR)` inside
   `publishResponse()` throws `JsonException` with **no** surrounding try/catch.
2. **Connection errors (B):** the guarded reply at `:318-320` catches only `\JsonException`; a
   `publish()->await()` connection-level failure (disconnect between request delivery and reply)
   escapes the same way.
3. **Unsanitized error code (C):** a handler throwing
   `new ServiceError("400\r\nInjected: x", ...)` — the description is CR/LF-collapsed (`:655`)
   but the **code** is not (`:656`), so `NatsHeaders::toWireBlock()` throws
   `InvalidArgumentException` from within `publishResponse()` (which at least prevents the
   header injection itself, but again escapes the callback).

Under `Service::run()` the escape is swallowed and retried after 20 ms; with a user-driven read
loop it surfaces as a spurious connection-level error and stalls sibling deliveries.

## Suggested fix

- Wrap **every** `publishResponse()` call in the dispatch closure in a `catch (\Throwable)` that
  records the error on the endpoint and emits it via the error listener (never rethrows).
- Sanitize/validate the `ServiceError` code like the description (or restrict to
  `[0-9A-Za-z_-]+`).
- Sanitize the correlation id (UTF-8 check or `JSON_INVALID_UTF8_SUBSTITUTE`) before embedding it
  in JSON payloads.
