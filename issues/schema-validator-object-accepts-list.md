# BasicJsonSchemaValidator accepts JSON arrays for `"type":"object"`

- **Status:** FIXED (2026-08-08) — `"type":"object"` now rejects non-empty JSON lists
  (`is_array && (empty || !array_is_list)`); the empty case stays ambiguous-accepted under assoc
  decoding. Confirmed by `testObjectTypeRejectsNonEmptyJsonList` (fails pre-fix).
- **Severity:** minor
- **Type:** correctness (validation gap)
- **Area:** Services / schema validation
- **Where:** `src/Services/BasicJsonSchemaValidator.php:83` (`'object' => is_array($value)`), and
  the `is_array($value)` gate at `:41`

## Problem

With schema `{"type":"object","properties":{...}}` and request payload `[1,2,3]`, `is_array()`
is true, so the type check passes and validation returns null — a JSON Schema validator must
reject a list here. The converse case is already handled correctly
(`'array' => is_array($value) && array_is_list($value)` at `:82`), so the distinction is
available: `object` should be `is_array($value) && !array_is_list($value)` (with the unavoidable
`{}`-vs-`[]` ambiguity of assoc-decoding applying only to the *empty* case, which may be
special-cased as acceptable).

Downstream effect: endpoints relying on the validator to gate handler input receive a list where
they expect a map; the required-properties check at `:44-48` then reports misleading
"is required" errors instead of a type error, or passes entirely when `required` is empty.

## Suggested fix

```php
'object' => is_array($value) && ($value === [] || !array_is_list($value)),
```

plus a unit test for the non-empty-list rejection.
