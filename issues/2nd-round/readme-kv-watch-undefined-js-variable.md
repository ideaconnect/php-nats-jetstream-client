# README KV watch example calls $js->stopOrderedConsumer() but the snippet never defines $js — copy-paste fatals

- **Status:** OPEN (filed 2026-08-08, second-round review; adversarially verified)
- **Severity:** minor
- **Type:** documentation (broken runnable example)
- **Area:** README (round-1 fix follow-up: teardown switched to stopOrderedConsumer)
- **Where:** `README.md:900` (the new teardown line), snippet block `:851-903` defines only
  `$client` and `$kv = $client->jetStream()->keyValue('cfg')`

## Problem

The KV watch example's new teardown line is `$js->stopOrderedConsumer($watchSid)->await();`, but
the fenced snippet never assigns `$js` (baseline used `$client->unsubscribe($watchSid)`). The
parallel edit in the ordered-consumer snippet (`:1276`) is fine because that snippet defines
`$js = $client->jetStream();` first — only the KV snippet was left broken.

## Failure scenario

A user copies the example verbatim: PHP 8 raises "Undefined variable $js" then fatals with
"Call to a member function stopOrderedConsumer() on null" at teardown — skipping
stopOrderedConsumer/deleteBucket/disconnect, leaving the watch consumer and watchdog timer
running.

## Suggested fix

Introduce `$js = $client->jetStream(); $kv = $js->keyValue('cfg');` at the top of the snippet
(or call `$client->jetStream()->stopOrderedConsumer($watchSid)`).
