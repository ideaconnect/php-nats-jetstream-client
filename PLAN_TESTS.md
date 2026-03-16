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
| C03 | Core Connection | Reconnect after active transport loss + replay | Yes | `testReconnectAfterTransportLossReplaysSubscriptions` | P0 | Covered |
| C04 | Core Connection | Reconnect attempts exhausted | Yes | `testReconnectAttemptsExhaustedReturnsClosed` | P1 | Covered |
| C05 | Core Connection | Exponential backoff/jitter reconnect behavior | Yes | `testReconnectBackoffDelayProgression` | P1 | Covered |
| C06 | Core Connection | Ping/pong liveness with maxPingsOut | Yes | `testMaxPingsOutTriggersReconnect` | P1 | Covered |
| C07 | Core Connection | Graceful drain with in-flight deliveries | Yes | `testDrainDuringInflightDelivery` | P1 | Covered |
| C08 | Core Connection | no_echo option | Yes | `testNoEchoSuppressesSelfPublishedMessages` | P0 | Covered |
| C09 | Core Connection | tlsHandshakeFirst workflow | Yes | `testTlsHandshakeFirstConnection` | P2 | Covered |
| C10 | Core Connection | max_payload enforcement | Yes | `testOversizedPublishIsRejected` | P1 | Covered |
| P01 | Pub/Sub | Publish-subscribe round trip | Yes | `testPublishAndSubscribeRoundTrip` | P0 | Covered |
| P02 | Pub/Sub | Header publish round trip | Yes | `testPublishWithHeadersRoundTrip` | P0 | Covered |
| P03 | Pub/Sub | Queue-group distribution semantics | Yes | `testQueueGroupDistributesMessages` | P1 | Covered |
| P04 | Pub/Sub | Subject wildcard subscription behavior | Yes | `testWildcardSubscriptionReceivesExpectedSubjects` | P2 | Covered |
| R01 | Request/Reply | Basic request/reply | Yes | `testRequestReply` | P0 | Covered |
| R02 | Request/Reply | Header propagation in requestWithHeaders | Yes | `testRequestWithHeadersPropagatesHeaders` | P0 | Covered |
| R03 | Request/Reply | Request timeout path integration | Yes | `testRequestTimeoutReturnsTimeoutError` | P1 | Covered |
| R04 | Request/Reply | Cancellation path integration | Yes | `testRequestCancellationStopsAwait` | P2 | Covered |
| J01 | JetStream Streams | Account info | Yes | `testJetStreamAccountAndStreamLifecycle` | P0 | Covered |
| J02 | JetStream Streams | Stream create/get/delete | Yes | `testJetStreamAccountAndStreamLifecycle` | P0 | Covered |
| J03 | JetStream Streams | Stream update config | Yes | `testJetStreamUpdateStreamConfiguration` | P1 | Covered |
| J04 | JetStream Streams | Stream purge | Yes | `testJetStreamPurgeStreamByFilter` | P1 | Covered |
| J05 | JetStream Streams | Stream list API | Yes | `testJetStreamListStreams` | P2 | Covered |
| J06 | JetStream Streams | Stream direct message get | Yes | `testJetStreamGetStreamMessage` | P1 | Covered |
| J07 | JetStream Streams | Stream retention/storage/discard policies | Yes | `testJetStreamStreamPoliciesPersist` | P1 | Covered |
| JC01 | JetStream Consumers | Durable consumer create/get/delete | Yes | `testJetStreamConsumerAndPublishAck` | P0 | Covered |
| JC02 | JetStream Consumers | Consumer list API | Yes | `testJetStreamListConsumers` | P2 | Covered |
| JC03 | JetStream Consumers | Consumer pause/resume | Yes | `testJetStreamPauseAndResumeConsumer` | P1 | Covered |
| JP01 | JetStream Pull | fetchNext + ack | Yes | `testJetStreamPullFetchAndAck` | P0 | Covered |
| JP02 | JetStream Pull | delayed NAK redelivery | Yes | `testJetStreamPullNakWithDelayRedelivery` | P0 | Covered |
| JP03 | JetStream Pull | fetchBatch and terminal statuses | Yes | `testJetStreamFetchBatchHandlesStatusFrames` | P1 | Covered |
| JP04 | JetStream Pull | TERM/WPI workflows | Yes | `testJetStreamTermAndInProgressTokens` | P2 | Covered |
| JP05 | JetStream Pull | Pull consumer iterator chaining | Yes | `testJetStreamPullIteratorBatching` | P2 | Covered |
| JPS01 | JetStream Push | Durable push helper delivery | Yes | `testJetStreamPushConsumerHelperDelivery` | P0 | Covered |
| JPS02 | JetStream Push | Explicit deliver subject | Yes | `testJetStreamPushConsumerWithExplicitDeliverSubject` | P0 | Covered |
| JPS03 | JetStream Push | Heartbeat/flow-control handling | Yes | `testJetStreamPushFlowControlAndHeartbeat` | P1 | Covered |
| JPS04 | JetStream Push | Ordered consumer recovery semantics | Yes | `testJetStreamOrderedConsumerWithFilteredSubjectAfterPriorMessages` | P0 | Covered |
| JPS05 | JetStream Push | Ephemeral push helper | Yes | `testJetStreamEphemeralPushConsumerDelivery` | P2 | Covered |
| JS01 | Scheduling | Scheduled publish `@at` delivery | Yes | `testJetStreamScheduledPublish` | P0 | Covered |
| JS02 | Scheduling | Invalid schedule expressions | Yes | `testJetStreamScheduledPublishRejectsUnsupportedPatterns` | P2 | Covered |
| KV01 | KeyValue | Bucket lifecycle create/delete | Yes | `testJetStreamKeyValueLifecycle` | P0 | Covered |
| KV02 | KeyValue | put/get/delete watch | Yes | `testJetStreamKeyValueLifecycle` | P0 | Covered |
| KV03 | KeyValue | update expected revision | Yes | `testJetStreamKeyValueAdvancedParityOperations` | P0 | Covered |
| KV04 | KeyValue | purge/getAll/status | Yes | `testJetStreamKeyValueAdvancedParityOperations` | P0 | Covered |
| KV05 | KeyValue | TTL/history semantics integration | Yes | `testJetStreamKeyValueHistoryAndTtlBehavior` | P1 | Covered |
| KV06 | KeyValue | concurrent watchers | Yes | `testJetStreamKeyValueConcurrentWatchers` | P2 | Covered |
| OS01 | Object Store | Bucket create/delete | Yes | `testJetStreamObjectStoreLifecycle` | P0 | Covered |
| OS02 | Object Store | put/get/info/delete/list/watch | Yes | `testJetStreamObjectStoreLifecycle` | P0 | Covered |
| OS03 | Object Store | Multi-chunk object retrieval | Yes | `testJetStreamObjectStoreLargeObjectChunks` | P1 | Covered |
| OS04 | Object Store | Digest mismatch path | Yes | `testJetStreamObjectStoreDigestMismatch` | P2 | Covered |
| S01 | Services | Service endpoint request/reply | Yes | `testServiceDiscoveryAndEndpoint` | P0 | Covered |
| S02 | Services | Discovery PING/INFO/STATS/SCHEMA contract | Yes | `testServiceDiscoverySubjectsContract` | P1 | Covered |
| S03 | Services | Observers and endpoint metrics | Yes | `testServiceStatsAndObserversWithHeaders` | P0 | Covered |
| S04 | Services | Multi-endpoint dispatch | Yes | `testServiceMultipleEndpoints` | P1 | Covered |
| S05 | Services | Grouped endpoint hierarchy | Yes | `testServiceGroupedEndpointsHierarchy` | P1 | Covered |
| S06 | Services | Concurrent request handling | Yes | `testServiceConcurrentRequests` | P1 | Covered |
| A01 | Auth/TLS | Token auth success/failure | Yes | `testTokenAuthSuccessAndFailure` | P2 | Covered |
| A02 | Auth/TLS | Username/password success/failure | Yes | `testUserPasswordAuthSuccessAndFailure` | P2 | Covered |
| A03 | Auth/TLS | JWT nonce auth integration | Yes | `testJwtNonceAuthenticationFlow` | P2 | Covered |
| A04 | Auth/TLS | Standalone NKey auth | Yes | `testStandaloneNkeyAuthenticationFlow` | P2 | Covered |
| A05 | Auth/TLS | TLS CA/cert/key flow | Yes | `testTlsHandshakeFirstConnection`, `testTlsConnectionFailsWithoutClientCertificate`, `testTlsConnectionFailsWithWrongCa`, `testTlsConnectionFailsWithPeerNameMismatch` | P2 | Covered |
| PR01 | Protocol | Fragmented frame handling end-to-end | Yes | `testFragmentedFramesStillDispatch` | P2 | Covered |
| PR02 | Protocol | no_responders behavior from server | Yes | `testNoRespondersErrorSurface` | P1 | Covered |
| PR03 | Protocol | Slow consumer policy integration behavior | Yes | `testSlowConsumerPolicyBehavior` | P2 | Covered |

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
