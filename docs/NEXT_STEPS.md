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
- [ ] JetStream Services API: service registration/discovery/request handling primitives.
- [ ] Parser robustness expansion: malformed/fragmented property-style tests.
- [ ] CI expansion: split fast unit/static pipeline and dockerized integration pipeline for push/PR.
- [ ] Server authorization methods: JWT, password.