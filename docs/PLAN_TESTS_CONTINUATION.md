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

Files already updated:
- tests/Integration/NatsClientIntegrationTest.php
- tests/Integration/JetStreamIntegrationTest.php
- PLAN_TESTS.md

## Next Recommended Work Order
Implement in this order to maximize risk reduction first:
1. S02 - service discovery subjects contract (PING/INFO/STATS/SCHEMA)
2. JPS03 - push flow-control and heartbeat handling
3. JP03 - fetchBatch and terminal status behaviors
4. C03 - reconnect after active transport loss with subscription replay
5. C06 - maxPingsOut triggers reconnect path
6. PR02 - no_responders integration behavior

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
- JP03, PR02

Batch C (connection resilience):
- C03, C06

Batch D (remaining Phase 1/2 IDs):
- J07, JC03, KV05, OS03
