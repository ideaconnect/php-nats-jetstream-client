# Integration Test Plan Matrix

Continuation handoff: see [docs/PLAN_TESTS_CONTINUATION.md](docs/PLAN_TESTS_CONTINUATION.md).

## Goal
Ensure every user-facing feature is covered by at least one integration scenario.

## Scope
- In scope: integration coverage planning and phased implementation.
- Out of scope: replacing unit tests or exhaustive performance benchmarking.

## Status Legend
- Covered: existing integration test already validates feature behavior.
- Planned: integration scenario defined and should be implemented.
- Partial: existing coverage exists but misses critical behavior branches.

## Domain Matrix

| ID | Domain | Feature | Current Integration Coverage | Test Target (existing/new) | Priority | Status |
| --- | --- | --- | --- | --- | --- | --- |
| C01 | Core Connection | Connect/disconnect handshake | Yes | `testConnectAndDisconnect` | P0 | Covered |
| C02 | Core Connection | Server rotation fallback | Yes | `testConnectWithServerRotationFallback` | P0 | Covered |
| C03 | Core Connection | Reconnect after active transport loss + replay | No | `testReconnectAfterTransportLossReplaysSubscriptions` (new) | P0 | Planned |
| C04 | Core Connection | Reconnect attempts exhausted | No | `testReconnectAttemptsExhaustedReturnsClosed` (new) | P1 | Planned |
| C05 | Core Connection | Exponential backoff/jitter reconnect behavior | No | `testReconnectBackoffDelayProgression` (new) | P1 | Planned |
| C06 | Core Connection | Ping/pong liveness with maxPingsOut | No | `testMaxPingsOutTriggersReconnect` (new) | P1 | Planned |
| C07 | Core Connection | Graceful drain with in-flight deliveries | Partial | `testDrainDuringInflightDelivery` (new) | P1 | Planned |
| C08 | Core Connection | no_echo option | Yes | `testNoEchoSuppressesSelfPublishedMessages` | P0 | Covered |
| C09 | Core Connection | tlsHandshakeFirst workflow | No | `testTlsHandshakeFirstConnection` (new, TLS env) | P2 | Planned |
| C10 | Core Connection | max_payload enforcement | No | `testOversizedPublishIsRejected` (new) | P1 | Planned |
| P01 | Pub/Sub | Publish-subscribe round trip | Yes | `testPublishAndSubscribeRoundTrip` | P0 | Covered |
| P02 | Pub/Sub | Header publish round trip | Yes | `testPublishWithHeadersRoundTrip` | P0 | Covered |
| P03 | Pub/Sub | Queue-group distribution semantics | No | `testQueueGroupDistributesMessages` (new) | P1 | Planned |
| P04 | Pub/Sub | Subject wildcard subscription behavior | No | `testWildcardSubscriptionReceivesExpectedSubjects` (new) | P2 | Planned |
| R01 | Request/Reply | Basic request/reply | Yes | `testRequestReply` | P0 | Covered |
| R02 | Request/Reply | Header propagation in requestWithHeaders | Yes | `testRequestWithHeadersPropagatesHeaders` | P0 | Covered |
| R03 | Request/Reply | Request timeout path integration | No | `testRequestTimeoutReturnsTimeoutError` (new) | P1 | Planned |
| R04 | Request/Reply | Cancellation path integration | No | `testRequestCancellationStopsAwait` (new) | P2 | Planned |
| J01 | JetStream Streams | Account info | Yes | `testJetStreamAccountAndStreamLifecycle` | P0 | Covered |
| J02 | JetStream Streams | Stream create/get/delete | Yes | `testJetStreamAccountAndStreamLifecycle` | P0 | Covered |
| J03 | JetStream Streams | Stream update config | Yes | `testJetStreamUpdateStreamConfiguration` | P1 | Covered |
| J04 | JetStream Streams | Stream purge | Yes | `testJetStreamPurgeStreamByFilter` | P1 | Covered |
| J05 | JetStream Streams | Stream list API | No | `testJetStreamListStreams` (new) | P2 | Planned |
| J06 | JetStream Streams | Stream direct message get | Yes | `testJetStreamGetStreamMessage` | P1 | Covered |
| J07 | JetStream Streams | Stream retention/storage/discard policies | No | `testJetStreamStreamPoliciesPersist` (new) | P1 | Planned |
| JC01 | JetStream Consumers | Durable consumer create/get/delete | Yes | `testJetStreamConsumerAndPublishAck` | P0 | Covered |
| JC02 | JetStream Consumers | Consumer list API | No | `testJetStreamListConsumers` (new) | P2 | Planned |
| JC03 | JetStream Consumers | Consumer pause/resume | No | `testJetStreamPauseAndResumeConsumer` (new) | P1 | Planned |
| JP01 | JetStream Pull | fetchNext + ack | Yes | `testJetStreamPullFetchAndAck` | P0 | Covered |
| JP02 | JetStream Pull | delayed NAK redelivery | Yes | `testJetStreamPullNakWithDelayRedelivery` | P0 | Covered |
| JP03 | JetStream Pull | fetchBatch and terminal statuses | No | `testJetStreamFetchBatchHandlesStatusFrames` (new) | P1 | Planned |
| JP04 | JetStream Pull | TERM/WPI workflows | No | `testJetStreamTermAndInProgressTokens` (new) | P2 | Planned |
| JP05 | JetStream Pull | Pull consumer iterator chaining | No | `testJetStreamPullIteratorBatching` (new) | P2 | Planned |
| JPS01 | JetStream Push | Durable push helper delivery | Yes | `testJetStreamPushConsumerHelperDelivery` | P0 | Covered |
| JPS02 | JetStream Push | Explicit deliver subject | Yes | `testJetStreamPushConsumerWithExplicitDeliverSubject` | P0 | Covered |
| JPS03 | JetStream Push | Heartbeat/flow-control handling | Partial | `testJetStreamPushFlowControlAndHeartbeat` (new) | P1 | Planned |
| JPS04 | JetStream Push | Ordered consumer recovery semantics | Yes | `testJetStreamOrderedConsumerWithFilteredSubjectAfterPriorMessages` | P0 | Covered |
| JPS05 | JetStream Push | Ephemeral push helper | No | `testJetStreamEphemeralPushConsumerDelivery` (new) | P2 | Planned |
| JS01 | Scheduling | Scheduled publish `@at` delivery | Yes | `testJetStreamScheduledPublish` | P0 | Covered |
| JS02 | Scheduling | Invalid schedule expressions | No | `testJetStreamScheduledPublishRejectsUnsupportedPatterns` (new) | P2 | Planned |
| KV01 | KeyValue | Bucket lifecycle create/delete | Yes | `testJetStreamKeyValueLifecycle` | P0 | Covered |
| KV02 | KeyValue | put/get/delete watch | Yes | `testJetStreamKeyValueLifecycle` | P0 | Covered |
| KV03 | KeyValue | update expected revision | Yes | `testJetStreamKeyValueAdvancedParityOperations` | P0 | Covered |
| KV04 | KeyValue | purge/getAll/status | Yes | `testJetStreamKeyValueAdvancedParityOperations` | P0 | Covered |
| KV05 | KeyValue | TTL/history semantics integration | No | `testJetStreamKeyValueHistoryAndTtlBehavior` (new) | P1 | Planned |
| KV06 | KeyValue | concurrent watchers | No | `testJetStreamKeyValueConcurrentWatchers` (new) | P2 | Planned |
| OS01 | Object Store | Bucket create/delete | Yes | `testJetStreamObjectStoreLifecycle` | P0 | Covered |
| OS02 | Object Store | put/get/info/delete/list/watch | Yes | `testJetStreamObjectStoreLifecycle` | P0 | Covered |
| OS03 | Object Store | Multi-chunk object retrieval | No | `testJetStreamObjectStoreLargeObjectChunks` (new) | P1 | Planned |
| OS04 | Object Store | Digest mismatch path | No | `testJetStreamObjectStoreDigestMismatch` (new) | P2 | Planned |
| S01 | Services | Service endpoint request/reply | Yes | `testServiceDiscoveryAndEndpoint` | P0 | Covered |
| S02 | Services | Discovery PING/INFO/STATS/SCHEMA contract | Partial | `testServiceDiscoverySubjectsContract` (new) | P1 | Planned |
| S03 | Services | Observers and endpoint metrics | Yes | `testServiceStatsAndObserversWithHeaders` | P0 | Covered |
| S04 | Services | Multi-endpoint dispatch | No | `testServiceMultipleEndpoints` (new) | P1 | Planned |
| S05 | Services | Grouped endpoint hierarchy | No | `testServiceGroupedEndpointsHierarchy` (new) | P1 | Planned |
| S06 | Services | Concurrent request handling | No | `testServiceConcurrentRequests` (new) | P1 | Planned |
| A01 | Auth/TLS | Token auth success/failure | No | `testTokenAuthSuccessAndFailure` (new, env-config) | P2 | Planned |
| A02 | Auth/TLS | Username/password success/failure | No | `testUserPasswordAuthSuccessAndFailure` (new, env-config) | P2 | Planned |
| A03 | Auth/TLS | JWT nonce auth integration | No | `testJwtNonceAuthenticationFlow` (new, env-config) | P2 | Planned |
| A04 | Auth/TLS | Standalone NKey auth | No | `testStandaloneNkeyAuthenticationFlow` (new, env-config) | P2 | Planned |
| A05 | Auth/TLS | TLS CA/cert/key flow | No | `testTlsMutualAuthConnection` (new, env-config) | P2 | Planned |
| PR01 | Protocol | Fragmented frame handling end-to-end | No | `testFragmentedFramesStillDispatch` (new) | P2 | Planned |
| PR02 | Protocol | no_responders behavior from server | No | `testNoRespondersErrorSurface` (new) | P1 | Planned |
| PR03 | Protocol | Slow consumer policy integration behavior | No | `testSlowConsumerPolicyBehavior` (new) | P2 | Planned |

## Implementation Phases

### Phase 1 (P0/P1 critical)
- Reconnect resilience (`C03`, `C06`)
- Headers semantics (`P02`, `R02`)
- no_echo (`C08`)
- Stream update/purge/direct get (`J03`, `J04`, `J06`)
- Push flow-control/heartbeat (`JPS03`)
- Service discovery contract (`S02`)

### Phase 2 (JetStream policy depth)
- Stream policy options (`J07`)
- Pull batch/term/wpi (`JP03`, `JP04`)
- KV TTL/history (`KV05`)
- Object large chunk and digest path (`OS03`, `OS04`)

### Phase 3 (advanced and environment-dependent)
- Auth/TLS matrix (`A01`..`A05`)
- Queue-group distribution and fragmentation (`P03`, `PR01`)
- Service concurrency/grouped endpoints (`S05`, `S06`)

## Test Design Rules
- Every integration test must use unique stream/consumer/bucket/subject names (random suffix).
- Every test must clean up created server resources in `finally`-safe style where applicable.
- Avoid ordering assumptions between tests.
- Keep each scenario deterministic with bounded retries for eventual consistency.

## Acceptance Criteria
- Every feature row in the matrix is either Covered or has an explicit Planned integration scenario.
- New implementation work should update this file status from Planned to Covered when merged.

## Checkpoint (March 13, 2026)
- Implemented and marked Covered in this execution cycle:
1. `P02` Header publish round trip
2. `R02` Header propagation in requestWithHeaders
3. `C08` no_echo option
4. `J03` Stream update config
5. `J04` Stream purge
6. `J06` Stream direct message get
- Integration suite currently includes these scenarios but skips unless `RUN_INTEGRATION=1` is set.
