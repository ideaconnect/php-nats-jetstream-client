# PLAN_TESTS Continuation Guide

## Purpose
Use this document to resume integration-test implementation from PLAN_TESTS.md with minimal re-discovery work.

## Current State (Checkpoint)
Completed in latest implementation pass:
- C08: no_echo option
- P02: header publish round trip
- R02: header propagation in requestWithHeaders
- J03: stream update config
- J04: stream purge
- J06: stream direct message get
- S02: service discovery subjects contract
- JPS03: push flow-control and heartbeat handling
- JP03: fetchBatch and terminal status behaviors
- C03: reconnect after active transport loss with subscription replay
- C06: maxPingsOut triggers reconnect path
- PR02: no_responders integration behavior
- J07: stream retention/storage/discard policies
- JC03: consumer pause/resume
- KV05: key-value TTL/history semantics
- OS03: object store multi-chunk retrieval
- C04: reconnect attempts exhausted
- C05: reconnect backoff/jitter behavior
- J05: stream list API
- JC02: consumer list API
- P03: queue-group distribution semantics
- R03: request timeout integration path
- C07: graceful drain with in-flight deliveries
- C10: max_payload enforcement
- P04: wildcard subscription behavior
- R04: request cancellation path integration
- JPS05: ephemeral push helper delivery
- JS02: invalid schedule expression rejection
- JP04: TERM/WPI pull-consumer workflows
- JP05: pull-consumer iterator batching
- KV06: concurrent key-value watchers
- OS04: object store digest mismatch path
- S04: service multi-endpoint dispatch
- S05: service grouped endpoint hierarchy
- S06: service concurrent request handling
- PR01: fragmented frame handling end-to-end
- PR03: slow-consumer policy behavior
- C09: tlsHandshakeFirst workflow
- A01: token auth success/failure
- A02: username/password auth success/failure
- A03: JWT nonce auth integration
- A04: standalone NKey auth integration

Files already updated:
- tests/Integration/NatsClientIntegrationTest.php
- tests/Integration/JetStreamIntegrationTest.php
- PLAN_TESTS.md

## Next Recommended Work Order
Implement in this order to maximize risk reduction first:
1. Capture any flaky integration behavior over repeated local compose-backed runs and manual CI soak runs
2. Consider documenting a minimal external JWT resolver setup for teams not using the committed local fixture
3. Decide whether the manual CI soak should become scheduled or stay operator-triggered only

## How To Continue (Workflow)
1. Pick one feature ID from PLAN_TESTS.md with `Status = Planned` or `Partial`.
2. Add integration test(s) in the most relevant file:
- Core/request/services: tests/Integration/NatsClientIntegrationTest.php
- JetStream/KV/ObjectStore: tests/Integration/JetStreamIntegrationTest.php
3. Use unique names for stream/consumer/subject/bucket in each test:
- Suffix pattern: `bin2hex(random_bytes(3))` (or 4 for subjects)
4. Always clean up resources (streams, consumers, buckets, subscriptions).
5. Keep bounded retry loops for eventual consistency and avoid tight spins.
6. After implementing, update PLAN_TESTS.md:
- `Current Integration Coverage`: No -> Yes (or Partial -> Yes)
- `Test Target`: replace `(new)` with implemented test method name
- `Status`: Planned/Partial -> Covered
7. Re-run validation commands.

## Validation Commands
Run structure/static checks first:
- `./vendor/bin/phpstan analyse tests/Integration/NatsClientIntegrationTest.php tests/Integration/JetStreamIntegrationTest.php`
- `./vendor/bin/phpunit --testsuite integration`

Run real integration scenarios when environment is enabled:
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testServiceDiscoverySubjectsContract`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testJetStreamPushFlowControlAndHeartbeat`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testJetStreamFetchBatchHandlesStatusFrames`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testReconnectAfterTransportLossReplaysSubscriptions`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testMaxPingsOutTriggersReconnect`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testNoRespondersErrorSurface`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testJetStreamStreamPoliciesPersist`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testJetStreamPauseAndResumeConsumer`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testJetStreamKeyValueHistoryAndTtlBehavior`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testJetStreamObjectStoreLargeObjectChunks`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testReconnectAttemptsExhaustedReturnsClosed`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testReconnectBackoffDelayProgression`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testJetStreamListStreams`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testJetStreamListConsumers`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testQueueGroupDistributesMessages`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testRequestTimeoutReturnsTimeoutError`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testDrainDuringInflightDelivery`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testOversizedPublishIsRejected`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testWildcardSubscriptionReceivesExpectedSubjects`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testRequestCancellationStopsAwait`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testJetStreamEphemeralPushConsumerDelivery`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testJetStreamScheduledPublishRejectsUnsupportedPatterns`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testJetStreamTermAndInProgressTokens`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testJetStreamPullIteratorBatching`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testJetStreamKeyValueConcurrentWatchers`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testJetStreamObjectStoreDigestMismatch`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testServiceMultipleEndpoints`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testServiceGroupedEndpointsHierarchy`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testServiceConcurrentRequests`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testFragmentedFramesStillDispatch`
- `RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter testSlowConsumerPolicyBehavior`

Optional explicit server URL:
- `RUN_INTEGRATION=1 NATS_URL=nats://127.0.0.1:14222 ./vendor/bin/phpunit --testsuite integration`

## Test Design Guardrails
- No inter-test ordering assumptions.
- Avoid flaky timing: use deadline loops (`microtime(true) + seconds`) and small sleeps.
- Use deterministic assertions focused on protocol outcomes, not exact timing.
- Prefer clear failure messages and domain-specific variable names.

## Definition Of Done Per Feature ID
- At least one integration test implemented and passing for the feature.
- PLAN_TESTS.md row updated to Covered.
- Integration suite remains green (or skipped without failures when RUN_INTEGRATION is not enabled).
- Static analysis passes for modified integration test files.

## Suggested Milestone Batches
Batch A (services + push behavior):
- S02, JPS03

Batch B (pull + protocol semantics):
- Completed: JP03, PR02

Batch C (connection resilience):
- Completed: C03, C06

Batch D (remaining Phase 1/2 IDs):
- Completed: J07, JC03, KV05, OS03

Batch E (core + listing + queue/timeout semantics):
- Completed: C04, C05, J05, JC02, P03, R03

Batch F (drain/payload/wildcard/cancellation + push/scheduling):
- Completed: C07, C10, P04, R04, JPS05, JS02

Batch G (pull token semantics + iterator batching):
- Completed: JP04, JP05

Batch H (KV/ObjectStore integrity edge cases):
- Completed: KV06, OS04

Batch I (service routing/grouping/concurrency):
- Completed: S04, S05, S06

Batch J (protocol resilience behaviors):
- Completed: PR01, PR03

Batch K (auth and TLS coverage):
- Completed: C09, A01, A02, A03, A04
