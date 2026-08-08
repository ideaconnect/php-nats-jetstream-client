# KV_-prefix idempotence guard applied to the `bucket` alias — a bucket legitimately named "KV_x" now sources/mirrors bucket "x"'s data instead

- **Status:** FIXED (2026-08-08) — the `bucket` alias is KV_-prefixed unconditionally in kvSourceConfig() and
  kvMirrorConfig() (bucket 'KV_x' maps to stream KV_KV_x with src transform on $KV.KV_x.>); the
  idempotence guard remains only for the `name` key and bare strings (nats.go stream-name
  semantics), documented in the docblocks. Pinned by
  `testCreateSourceBucketAliasIsKvPrefixedUnconditionally` and
  `testCreateMirrorBucketAliasIsKvPrefixedUnconditionally` (both pre-fix red).
- **Severity:** minor
- **Type:** wrong-bucket data routing (regression vs baseline for KV_-prefixed bucket names)
- **Area:** KV sources/mirror name mapping (round-1 ADR-57 fix follow-up)
- **Where:** `src/JetStream/KeyValue/KeyValueBucket.php:158-162` (alias copied into name),
  `:178-184` (idempotence guard), `:171` (same guard on the pass-through branch),
  `:218-221` (`kvMirrorConfig` same guard)

## Problem

The new idempotence guard skips prefixing when a name already starts with `KV_`:

```php
if (str_starts_with($name, 'KV_')) { $sourceBucket = substr($name, 3); }
else { ... $source['name'] = 'KV_' . $name; }
```

It is applied not only to the `name` key (where nats.go-parity idempotence makes sense — nats.go's
`Name` field is ambiguous) but also to the **`bucket` alias** and to bare-string source/mirror
entries. `KV_x` is a valid bucket name per ADR-8 (`\A[a-zA-Z0-9_-]+\z`), so
`['bucket' => 'KV_x']` now resolves to stream `KV_x` — the backing stream of bucket **x** — with
transform src `$KV.x.>`, instead of `KV_KV_x`. The alias *explicitly declares a KV bucket* (per
the round-1 issue doc itself), so bucket-name semantics should be unambiguous; nats.go has no
`bucket` alias to need this guard. Baseline HEAD prefixed unconditionally and handled `KV_x`
correctly.

## Failure scenario

Two buckets exist: `x` and `KV_x`.
`$js->keyValue('agg')->create(['sources' => [['bucket' => 'KV_x']]])` creates stream `KV_agg`
sourcing stream `KV_x` with transform `{src: '$KV.x.>', dest: '$KV.agg.>'}` — it aggregates bucket
**x**'s records; bucket `KV_x`'s records (under `$KV.KV_x.>` in stream `KV_KV_x`) are never
copied, with no error anywhere. Same wrong-bucket resolution for `'mirror' => ['bucket' => 'KV_x']`
(where `applyMirrorPrefixes` then routes mirror **writes** to the wrong bucket's live subjects)
and for bare-string `'sources' => ['KV_x']`.

## Suggested fix

Keep the idempotence guard for the `name` key (nats.go stream-name semantics) but prefix the
`bucket` alias unconditionally (`'KV_' . $bucket`) in both `kvSourceConfig()` and
`kvMirrorConfig()`, deriving `$sourceBucket` directly from the alias value; document that
bare-string entries follow nats.go's name semantics.
