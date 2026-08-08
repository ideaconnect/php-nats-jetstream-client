# A stop that races a recreate whose attempts then exhaust emits a spurious terminal "recreate failed" error for a deliberately stopped consumer

- **Status:** OPEN (filed 2026-08-08, second-round review; adversarially verified — found
  independently by two lenses)
- **Severity:** minor
- **Type:** spurious operator signal (false alarm)
- **Area:** JetStream ordered consumers / stopOrderedConsumer (round-1 fix follow-up)
- **Where:** `src/JetStream/JetStreamContext.php:1470` (stopped re-checked only after successful
  create), `:1521-1522` (attempt exhaustion throw), `:1533` (deferral excludes stopped),
  `:1554-1563` (unconditional `emitClientError`)

## Problem

The recreate retry loop re-checks `$state->stopped` only **after a successful create** (`:1470`).
A stop latched during a parked create await or the inter-attempt `delay()` whose remaining
attempts fail reaches `throw $e` (`:1521`). In the catch, the deferral condition
`state() !== Open && !$state->stopped` explicitly excludes stopped consumers (and is skipped
anyway when the connection is Open, e.g. repeated JS API 503s), so execution unconditionally
reaches

```php
$this->emitClientError(new JetStreamException(sprintf(
    'Ordered consumer recreate failed for stream "%s" after %d attempts...', ...)));
```

— a terminal "recreate failed" error, forwarded unfiltered to the logger and error listener, for a
consumer the user just deliberately stopped. The existing test
`testStopOrderedConsumerDuringInFlightRecreateDoesNotResurrect` pins only the create-succeeds arm
(silent teardown); no test or contract covers the all-attempts-fail-after-stop arm.

## Failure scenario

An operator calls `stopOrderedConsumer()` on a KV watch during a leadership election while a
recreate is parked in its create awaits; the remaining create attempts fail (JS API 503s).
`stop()` returns normally, but the application's error listener then receives
"Ordered consumer recreate failed for stream ... after 3 attempts" — paging on-call for a consumer
that was intentionally shut down. (Teardown itself is harmless: double unsubscribe/deregister are
no-ops, and this path releases the never-adopted `$newSid`.)

## Suggested fix

In the catch, handle stopped first — `if ($state->stopped) { /* release $newSid best-effort */
return; }` before the deferral check and the `emitClientError` — mirroring the silent post-create
stopped teardown at `:1470-1484`. Optionally also re-check `stopped` between retry attempts to
stop burning them.
