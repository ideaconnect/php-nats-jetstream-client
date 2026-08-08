# NatsHeaders::toWireBlock() silently rewrites header values (trims leading/trailing whitespace)

- **Status:** FIXED (2026-08-08) — `toWireBlock()` emits header values VERBATIM (nats.go parity);
  the client's own decoder keeps its trim as a documented inbound tolerance. Confirmed by
  `testHeaderValueIsEmittedVerbatimOnTheWire` (fails pre-fix).
- **Severity:** minor
- **Type:** interop asymmetry / silent value mutation
- **Area:** Core headers
- **Where:** `src/Core/NatsHeaders.php:40-44`

## Problem

```php
$lines[] = $name . ':' . trim($singleValue);
```

Publishing `['X-Sig' => ' abc ']` emits `X-Sig:abc` — bytes the caller supplied are altered
before hitting the wire. The reference Go client writes the value verbatim after `: ` and
preserves trailing whitespace on decode, so the same publish through nats.go delivers `" abc "`
where this client delivers `"abc"`. The trim is documented as deliberate (round-trip symmetry
with the client's own decoder), but it silently mutates values a signature/checksum-carrying
header would trip over, and it diverges from other clients' wire output.

## Suggested fix

Emit the value verbatim (`$name . ':' . $singleValue`) and keep the tolerance on the *decode*
side only, or at minimum document the mutation prominently on `publishWithHeaders()` and reject
(rather than silently alter) values with significant surrounding whitespace.
