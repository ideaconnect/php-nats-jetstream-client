# Next Steps

## TODO Checklist

- [x] Protocol parser/reader base: control frames + MSG/HMSG with fragmented input handling.
- [x] Subscription runtime: SID registry, unsubscribe, dispatch buffering, and slow-consumer policies.
- [x] Request/reply: inbox generation, timeout/cancellation handling, and edge-case tests.
- [x] Connection resiliency: reconnect backoff, server rotation, and resubscribe replay.
- [x] Integration harness: Docker-based NATS 2.12+ integration for core connect/pub/sub/request/reconnect.
- [x] JetStream foundation: account info, stream CRUD, consumer CRUD, and publish ack.
- [x] Scheduled publish: NATS 2.12+ scheduling with `@at` support and helper (`Schedule::at`, `Schedule::atTimestamp`).

- [x] JetStream pull-consumer workflow: fetch-next API, ack/nak/term/in-progress helpers, delayed NAK support, and integration tests.
- [x] JetStream push-consumer flow control: durable push helpers, heartbeat/flow-control handling, and tests.
- [x] JetStream KV API: bucket lifecycle + put/get/delete/watch with unit/integration coverage.
- [x] JetStream Object Store API: bucket/object operations with streaming and metadata tests.
- [x] JetStream Services API: service registration/discovery/request handling primitives.
- [x] Parser robustness expansion: malformed/fragmented property-style tests.
- [x] CI expansion: split fast unit/static pipeline and dockerized integration pipeline for push/PR.
- [x] Server authorization methods: JWT, password.
- [x] README regression pass vs basis-company/nats.php examples: parity matrix, equivalent mappings, and regression tests per section.
- [x] Stream purge, list streams, list consumers APIs.
- [x] `no_echo` and `tlsHandshakeFirst` connection options.
- [x] Max frame size limit in protocol parser (DoS protection).
- [x] Credentials file parser (`CredentialsParser`).
- [x] Typed JetStream enums: `RetentionPolicy`, `StorageBackend`, `DiscardPolicy`, `DeliverPolicy`, `AckPolicy`, `ReplayPolicy`.
- [x] Direct message get from stream (`getStreamMessage()`).
- [x] README: NAK/TERM/WPI examples, queue group example, processIncoming/reconnect docs, ordered consumer gap recovery docs.
- [x] Standalone NKey authentication (Ed25519 challenge signing without JWT).
- [x] Queue-based subscribe polling API (`SubscriptionQueue` with `fetch()`/`next()`/`fetchAll()`).
- [x] Consumer batching/iteration chain API (`PullConsumerIterator` with `setBatching()`/`setIterations()`/`handle()`).
- [x] Stream mirroring/sourcing configuration helpers (`StreamSource`).
- [x] Republish/subject transform configuration helpers (`Republish`, `SubjectTransform`).

## Remaining

No remaining items from the original roadmap.

## On-Hold

- [-] Schedule expressions beyond `@at` (cron, every, interval).