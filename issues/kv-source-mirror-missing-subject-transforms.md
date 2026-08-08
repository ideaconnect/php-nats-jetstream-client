# KV buckets created with `sources` omit the mandatory subject transforms; `mirror` buckets get no origin-prefix read/write mapping

- **Status:** FIXED (2026-08-07) — implemented to nats.go / ADR-57 parity (verified against the
  actual nats.go sources): every KV source gets the mandatory
  `{src: "$KV.<src>.>", dest: "$KV.<bucket>.>"}` transform (caller-supplied transforms pass through
  verbatim; external-same-bucket rule honored); mirrors get `mirror_direct: true`, no subjects, and
  runtime write-through to the origin prefix (`putPre` parity; cross-domain via
  `<external api>.$KV.<origin>.` with origin-prefixed reads); `domain` converts to
  `external: {api: "$JS.<domain>.API"}`; new `KeyValueBucket::bind()` resolves the same prefixes
  from STREAM.INFO for handles attached to mirrors created elsewhere. Confirmed by five unit tests
  (`testCreateWithSourcesAndExtendedConfig`, `testCreateSourceWithCustomTransformsPassesThroughVerbatim`,
  `testCreateMirrorEnablesMirrorDirectAndWritesThroughToOrigin`,
  `testCreateCrossDomainMirrorRoutesWritesAndReadsThroughOrigin`,
  `testBindResolvesMirrorPrefixesFromStreamInfo` — 4 of 5 fail pre-fix) plus the full live
  JetStream integration suite.
  **Known caveat (documented on `keyValue()`/`bind()` and in the CHANGELOG):** prefix resolution
  is per-handle — only the instance that ran `create()` is auto-resolved; any other handle to a
  mirror bucket (including a fresh `keyValue()` in the same process) must call `bind()` first.
  nats.go resolves on every bucket access via STREAM.INFO; auto-resolution here would add a
  round-trip to every bucket construction and is left as a possible follow-up. The `bucket` alias
  is also KV_-prefixed even when custom transforms are supplied (it explicitly declares a KV
  bucket; verbatim pass-through is reserved for `name`).
- **Severity:** major
- **Type:** spec correctness / silent data invisibility
- **Area:** KeyValue bucket creation
- **Where:** `src/JetStream/KeyValue/KeyValueBucket.php:89-132` (`create()` + `kvSourceConfig()`);
  no `subject_transforms` anywhere in `src/`

## Problem

`kvSourceConfig()` only translates a bucket name to the backing `KV_<name>` stream name. The
reference clients do more:

- **Sources:** nats.go `CreateKeyValue` attaches
  `SubjectTransforms: [{src: "$KV.A.>", dest: "$KV.B.>"}]` to every KV source, so sourced
  records are re-subjected into the new bucket's own prefix. Without the transform, sourced
  messages are copied into stream `KV_B` still bearing subject `$KV.A.<key>`. Every read path
  in this client — `get()`, `getAll()`, `keys()`, `watch()` — filters on `$KV.B.>`, so **every
  sourced entry is invisible** (get returns null, enumerations omit it, watch never fires) even
  though the data is in the stream. Silent divergence from what nats.go / the `nats` CLI produce
  for the same call.
- **Mirror:** the mirrored bucket's stream is created with `subjects: []` (correct) but the
  client keeps reading and writing under its *own* prefix. nats.go sets the put-prefix to
  `$KV.<origin>.` for mirror buckets. Here `put()`/`delete()` publish to `$KV.B.<key>` — a
  subject no stream ingests → JetStream 503 no-responders; reads look up `$KV.B.<key>` against
  records stored under `$KV.<origin>.<key>` → null for keys that exist upstream.

## Suggested fix

- For each source entry, add `subject_transforms: [{src: '$KV.<srcBucket>.>', dest: '$KV.<thisBucket>.>'}]`
  (respecting any caller-provided transforms).
- For mirror buckets, resolve the origin bucket and route `put`/`delete`/`get`/`watch` subjects
  through the origin prefix (nats.go `putPre` semantics), or reject writes on mirror buckets
  explicitly if write-through is not intended.
