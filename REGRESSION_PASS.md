# Regression Pass: basis-company README Examples

Goal: cover the same example scenarios from basis-company/nats.php README with this library, either as direct API match or documented equivalent mapping, with tests.

Reference:
- https://raw.githubusercontent.com/basis-company/nats.php/main/README.md

## Status Legend
- matched: direct scenario exists with tests and README example
- equivalent: scenario exists with different API shape; mapping documented and tested
- missing: no implementation yet

## Example Matrix

### Connecting and Auth
- Basic connect and ping-style handshake: matched
- Username/password auth: matched
- Token auth: matched
- JWT + nonce signing: equivalent
- TLS CA/cert/key options: matched

### Publish and Subscribe
- Publish + callback subscribe + process loop: matched
- Queue group usage: matched
- Header publish example: matched
- Queue fetch / fetchAll style API: equivalent

### Request and Response
- Request/reply async flow: equivalent
- Synchronous request/reply: matched

### JetStream API
- Account info + stream create/info/delete: matched
- Durable consumer create/info/delete: matched
- Pull fetch + ack/nak/term/in-progress: matched
- Ephemeral consumer examples: matched
- Scheduled publish example: equivalent

### Microservices
- Service registration and endpoint handling: matched
- Discovery ping/info/stats: matched
- Grouped endpoint hierarchy: matched

### Key Value Storage
- put/get/delete/watch basics: matched
- optimistic update with revision: matched
- purge key history: matched
- list/getAll + status examples: matched

### Object Store
- bucket lifecycle + put/get/info/delete: matched
- object listing parity: matched

### Performance and Config Docs
- performance benchmark recipe: matched
- configuration option mapping table: matched

## Initial Implementation Slice (started)

1. Added parity tracker roadmap item in docs/NEXT_STEPS.md.
2. Added this workspace plan file.
3. Started auth parity hardening with explicit token auth regression test.
4. Next code slice:
- add README compatibility matrix section
- add config mapping table
- add missing KV parity methods (purge/getAll/status/update)
- add grouped microservice routing decision

## Test Plan Skeleton

- Unit regression tests
- tests/Unit/Regression/AuthParityTest.php
- tests/Unit/Regression/JetStreamParityTest.php
- tests/Unit/Regression/KvParityTest.php

- Integration regression tests
- tests/Integration/Regression/ConnectAuthParityIntegrationTest.php
- tests/Integration/Regression/JetStreamParityIntegrationTest.php

## Acceptance Criteria

1. Every reference README section is tagged matched, equivalent, or missing.
2. Every matched/equivalent row points to concrete tests in this repository.
3. Missing rows are represented in docs/NEXT_STEPS.md as actionable tasks.
4. Regression suites run in CI with unit and integration jobs.
