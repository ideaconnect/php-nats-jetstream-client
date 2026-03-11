# Next Steps

## Completed First

1. Added unit tests for protocol codec, connection flow, and client facade.
2. Added a deterministic fake transport for isolated connection tests.
3. Verified baseline with static analysis and unit tests.
4. Added protocol frame parser for control frames, MSG, and HMSG with fragmented input handling tests.
5. Improved connect handshake to handle +OK, PING/PONG exchange, and early -ERR responses.

## Immediate Next Implementation Steps

1. Protocol reader loop
- Parse server frames beyond INFO/PONG: MSG, HMSG, PING, +OK, -ERR. (completed for parser layer)
- Introduce a frame model and parser tests for partial/buffered reads.

2. Subscription runtime
- Add subscribe and unsubscribe APIs with SID registry. (completed)
- Dispatch incoming messages to async handlers with bounded buffering. (completed with slow-consumer policies)

## Next Active Work Item

1. Implement request/reply API with inbox generation and timeout/cancellation behavior. (completed)
2. Add inbox prefix configurability and request edge-case handling tests. (completed)
3. Add request cancellation support and dedicated cancellation tests. (completed)
4. Add Docker-based integration harness for connect/publish/subscribe/request/reconnect scenarios. (completed)
5. Start JetStream foundation: context + account info + stream CRUD. (completed initial slice)
6. Extend JetStream foundation with consumer CRUD and publish-ack flows. (completed)
7. Add scheduled messages support and tests for NATS 2.12+. (completed, currently `@at` only)
8. Add schedule helper for valid `@at` generation (`Schedule::at`, `Schedule::atTimestamp`). (completed)

3. Request/reply support
- Add inbox generator and request API with timeout/cancellation.
- Map replies to pending requests with correlation by reply subject/SID.

4. Connection resiliency
- Add reconnect strategy (backoff + jitter) and server rotation. (completed)
- Re-issue SUB commands after reconnect and restore in-flight state where possible. (completed for subscriptions)

5. Integration testing
- Add Docker-based NATS 2.12+ integration setup. (completed)
- Add integration tests for connect/publish/subscribe/request and reconnect. (completed initial set)

6. JetStream foundation
- Add JetStream context and API request helper.
- Implement account info, stream CRUD, consumer CRUD, and publish with ack. (completed initial set)
- Add scheduled messages tests for NATS 2.12+ behavior. (completed for `@at`)

## Testing Expansion Plan

1. Keep unit tests deterministic with FakeTransport for protocol and state machine logic.
2. Add parser property tests for malformed and fragmented input.
3. Add integration tests that validate behavior against real NATS servers.
4. Add CI split: unit on every push, integration on push and PR with Docker.
