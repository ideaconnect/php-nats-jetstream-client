# Test Catalog

Every automated test in the suite with a one-line description of what it verifies. When you add or change a test, update the matching entry here. The per-file sections below are authoritative; the counts are indicative.

## How to run

- **Unit** (no server): `composer test:unit` - protocol encode/parse, connection state/handshake/reconnect, subscriptions & backpressure, and JetStream/KV/ObjectStore/Services logic exercised against in-memory transport doubles (`tests/Support`).
- **Integration** (live server): `RUN_INTEGRATION=1 composer test:integration`, or `composer test:e2e` for the full Dockerised stack (TLS/auth/WebSocket variants). Real connect/auth/TLS/WebSocket, JetStream/KV/ObjectStore/Services round-trips, reconnect, heartbeat soak, multi-consumer concurrency, and `nats` CLI interop.
- **Behat** (live server): `composer test:bdd` - behaviour specs.

Indicative totals: 2022 unit tests, 138 integration tests, 47 Behat scenarios.

## Unit Tests (`tests/Unit/`)

### tests/Unit/AmpSocketTransportTest.php
- `testWriteWithoutSocketThrowsWhileReadAndCloseStaySafe` - Asserts write() without a connected socket throws TransportClosedException (a silent no-op would confirm frames that never reached any socket, #124), while readLine() still returns '' and close() stays idempotent.
- `testUpgradeTlsIsNoOpWhenTlsNotConfigured` - Asserts upgradeTls() short-circuits (no handshake) when no socket and no TLS context are configured, leaving readLine() returning ''.
- `testWithTlsContextReturnsOriginalContextWhenTlsNotRequired` - Asserts withTlsContext() returns the same ConnectContext unchanged for a non-TLS nats:// DSN with tlsRequired=false.
- `testWithTlsContextBuildsTlsContextFromTlsScheme` - Asserts a tls:// DSN makes withTlsContext() return a new (different) ConnectContext with TLS enabled.
- `testWithTlsContextUsesExplicitTlsOptions` - With explicit TLS options (peer override, no peer verify, CA/cert/key files, passphrase) asserts withTlsContext() returns a new ConnectContext instance, exercising the explicit-options path.
- `testConnectThrowsOnInvalidDsn` - Asserts connect() propagates a Throwable for an invalid DSN.
- `testNormalizeSocketUriRewritesTlsScheme` - Asserts normalizeSocketUri() rewrites tls:// to tcp:// and leaves tcp:// unchanged, while accepting nats:// directly as tcp://.
- `testReadLineThrowsTransportClosedOnPeerEof` - With a server that writes "hello\r\n" then closes, asserts readLine() returns the data and the subsequent read throws TransportClosedException on EOF (not collapsing to '').
- `testReadLineSurfacesReadTimeoutAsCancelledExceptionNotEof` - With a peer held open but silent, asserts a bounded readLine() whose TimeoutCancellation fires surfaces CancelledException through the returned future - not EOF/TransportClosedException and not '' - guarding the #163 inline readLine() (no async() wrapper) so the passed Cancellation still reaches the underlying read and a bounded read stays distinguishable from a peer close.
- `testUpgradeTlsThrowsWhenConnectedWithoutTlsContext` - With a held-open plaintext connection, asserts upgradeTls() fails fast with TlsRequiredException ("TLS upgrade requested but no TLS context") rather than leaving the socket plaintext.
- `testReadChunkSizeControlsMaxBytesPerRead` - Over a real loopback socket, writes a 256 KiB payload and asserts the configured `readChunkSizeBytes` bounds each read: a 128 KiB chunk size lets a single readLine() return more than 8 KiB (and uses fewer total reads), while an 8 KiB chunk size caps every read at 8 KiB; both deliver the full payload (#119). Falsifiable: without the setChunkSize() call the transport stays at Amp's 8 KiB default and the >8 KiB assertion fails.

### tests/Unit/BasicJsonSchemaValidatorTest.php
- `testObjectTypeRejectsNonEmptyJsonList` - Asserts a non-empty JSON list (`[1,2,3]`) is rejected by a `type: object` schema even though PHP's assoc decoding turns both maps and lists into arrays, while a real map (`{"id":5}`) still validates and the empty `{}` stays accepted because `{}` and `[]` are indistinguishable after assoc decoding.
- `testRejectsInvalidJsonPayload` - Asserts validating a message with a malformed JSON body (`{invalid`) against an object schema returns the error `payload is not valid JSON`.
- `testRejectsMissingRequiredField` - Asserts a payload missing a required `id` field returns `$.id is required`.
- `testRejectsWrongPropertyType` - Asserts a payload where `id` is a string but the schema requires integer returns `$.id must be integer, got string`.
- `testAcceptsValidPayload` - Asserts a payload satisfying all required fields and property types validates successfully (returns null).
- `testValidatesAdditionalPrimitiveTypes` - Asserts boolean/number/null payloads validate against their matching primitive type schemas (null), and that a JSON object validated against `array` type yields `$ must be array, got array`.
- `testUnknownTypeIsIgnored` - Asserts a schema with an unrecognized `type` value (`custom-type`) is ignored and validation passes (returns null).
- `testRejectsObjectTypeWhenPayloadIsNotObject` - Asserts a plain JSON string payload validated against an `object` schema returns `$ must be object, got string`.
- `testIgnoresMalformedRequiredAndPropertiesSchemaNodes` - Asserts malformed schema nodes (a scalar `required`, a non-array property definition, a non-string property key) are tolerated and validation passes (returns null).
- `testSkipsPropertyNotPresentInObject` - Asserts an optional property declared in `properties` but absent from the payload and not in `required` is skipped without a type error (returns null).

### tests/Unit/BatchPublisherTest.php
- `testCommitPreflightRejectsPre212ServerBeforeAnyWrite` - With INFO advertising version 2.11.4, `commit()` on a 3-message batch throws UnsupportedFeatureException (feature allow_atomic, requiredVersion 2.12, serverVersion 2.11.4) from the version pre-flight with ZERO batch frames written - the scripted plain PubAck reply stays unconsumed, so no orphan start message can be stored (#152).
- `testCommitPreflightRejectsTwoSegmentPre212Version` - A two-segment INFO version ("2.11") parses as below 2.12 and the pre-flight rejects even a single-message batch before any write - previously its plain PubAck would have been silently accepted as a normal publish (#152).
- `testCommitProceedsOnPrereleaseOf212` - INFO version "2.12.0-beta.1" passes the pre-flight (numeric-prefix comparison, not semver precedence where a pre-release orders below its release) and a 2-message batch commits normally (batchCount=2, batchId="b-beta") (#152).
- `testCommitUnparseableVersionFallsThroughToReplyShapeDetection` - An unparseable INFO version ("synadia-custom") does not trip the pre-flight; the plain-PubAck start reply is caught by the reply-shape detection instead: UnsupportedFeatureException with serverVersion "synadia-custom" after exactly 1 batch write (#152 keeping #130 as defense in depth).
- `testCommitBeforeConnectSkipsVersionPreflightNullSafely` - Asserts `commit()` on a never-connected client (serverInfo() is null) skips the version pre-flight null-safely rather than dereferencing the missing ServerInfo, so the failure surfaces as the request layer's NatsException and not an UnsupportedFeatureException (#152).
- `testReplyShapeAbortMessageOmitsMissingServerVersionCleanly` - Asserts that when INFO omits `version`, the reply-shape abort's UnsupportedFeatureException message reads exactly "Atomic batch publish requires NATS server 2.12+ (connected server treated the batch as plain publishes)" - an unknown version leaves no blank token or dangling space in the wording (#152).
- `testCommitAbortsWhenBatchStartIsAcknowledgedAsPlainPublish` - Mixed-version cluster: the connected server advertises 2.12.1 (pre-flight passes) but the batch START is acknowledged with a normal PubAck (older JS leader storing it as a plain publish) - aborts with UnsupportedFeatureException (feature allow_atomic, server version reported) before any further batch message is published (#130).
- `testCommitRejectsMultiMessageAckWithoutBatchFields` - A multi-message commit acknowledged by a PubAck without batch id/count (nothing was committed as a batch) throws UnsupportedFeatureException instead of reporting success, with the INFO version at 2.12.1 and both batch writes asserted on the wire so the commit-leg guard (not the #152 pre-flight) is what fires (#130).
- `testSingleMessageBatchAcceptsPlainPubAck` - A single-message batch (trivially atomic) accepts a plain PubAck commit ack without batch fields (#130).
- `testCommitNormalizesNoRespondersTo503` - A no-responders reply to the commit request (single-message batch) surfaces as JetStreamException(503) ("No JetStream responder"), not a bare NatsException, matching jsRequest()'s taxonomy (#161).
- `testStartRequestNormalizesNoRespondersTo503` - A no-responders reply to the batch-START request (multi-message batch) likewise surfaces as JetStreamException(503); the batch aborts at start so no intermediate/commit frame is sent (#161).
- `testCommitSendsBatchHeadersAndParsesAck` - Commits a 3-message batch and asserts the parsed ack (batchCount=3, batchId="batch-xyz"), that exactly 3 writes carry `Nats-Batch-Id:batch-xyz`, the START (seq 1) is a request with no commit marker, the intermediate (seq 2) is fire-and-forget, and the commit (seq 3) is a request carrying `Nats-Batch-Commit:1`.
- `testCommitCoalescesIntermediatesIntoSingleWrite` - A 5-message commit performs exactly 3 batch transport writes: the start request, ONE coalesced write whose bytes are the exact concatenation of the intermediate HPUB frames (sequences 2-4, no reply inbox, staging order), and the commit request - instead of one awaited write per intermediate (#138).
- `testOversizedIntermediateThrowsBeforeAnyIntermediateWrite` - With the server advertising `max_payload:256`, an oversized intermediate (300-byte payload + batch headers) throws `ProtocolException` ("exceeds server max_payload") before ANY intermediate reaches the wire - only the start frame is written, no seq-2 frame and no commit marker appear in any write (#138).
- `testCommitRejectedAtStart` - When the batch-start request gets an error JSON reply, `commit()` throws `JetStreamException` ("atomic publish not enabled") and no commit marker nor seq-3 message is written (publish aborts at start).
- `testCommitEmptyBatchThrows` - Committing a batch with no staged messages throws `JetStreamException` ("Cannot commit an empty batch").
- `testBatchRejectsOversizedId` - Constructing a batch with a 65-character id throws `JetStreamException` ("Batch id must be between 1 and 64 characters").
- `testAddAfterCommitThrows` - Calling `add()` after a successful `commit()` throws `JetStreamException` ("Cannot add to an already-committed batch").
- `testCommitAbortSurfacesError` - A commit ack containing an error object surfaces as `JetStreamException` ("batch consistency check failed").
- `testAddExceedingMaxMessagesThrows` - Pre-filling the batch to `BatchPublisher::MAX_MESSAGES` (via reflection) then calling `add()` throws `JetStreamException` ("Atomic batch is limited to").
- `testCountReturnsNumberOfStagedMessages` - `count()` returns 0 initially and increments to 1 then 2 as messages are added.
- `testBatchIdReturnsConstructedId` - `batchId()` returns the id passed to the constructor ("my-explicit-id").
- `testDoubleCommitThrows` - First `commit()` succeeds (batchCount=1); a second `commit()` throws `JetStreamException` ("Batch already committed").
- `testCommitReleasesStagedPayloads` - `commit()` releases the staged payloads: the internal `messages` array is empty afterwards (reflection) and `count()` returns 0, while the full 3-message wire exchange still happens (#133).
- `testNonJsonStartReplyTreatedAsAccepted` - A non-empty, non-JSON start reply ("OK") is treated as accepted so publish continues, and the commit ack parses correctly (batchCount=2, batchId="batch-nonjson").
- `testMalformedCommitAckThrows` - A non-JSON commit ack reply throws `JetStreamException` ("Malformed atomic batch commit ack").

### tests/Unit/ConfigurationBuildersTest.php
- `testStreamConfigurationMapsEverySetter` - Chains every StreamConfiguration setter and asserts toArray() emits the correct wire keys, including seconds->ns conversions for max_age (60->60e9) and duplicate_window (120->120e9), boolean flags, compression, metadata, and a raw set('first_seq', 42).
- `testStreamConfigurationDefaultsToEmptySubjects` - Asserts a freshly created StreamConfiguration('EMPTY') serializes with its name and an empty subjects array by default.
- `testConsumerConfigurationMapsEverySetter` - Chains every ConsumerConfiguration setter and asserts toArray() maps to wire keys with ms->ns conversions for ack_wait (5000->5e9), inactive_threshold (30000->30e9), and each backoff element ([1000,2000]->[1e9,2e9]), plus filter_subjects, policies, metadata, and raw set('rate_limit_bps').
- `testConsumerConfigurationEphemeralHasNoDurableName` - Asserts an ephemeral consumer (ackPolicy None, no durable) has null name, omits the durable_name key from toArray(), and emits ack_policy 'none'.

### tests/Unit/CredentialsParserTest.php
- `testParseExtractsJwtAndNkeySeed` - Asserts `parse()` extracts the JWT and NKey seed from a standard five-dash BEGIN/END `.creds` block.
- `testParseAcceptsCanonicalNscMarkersWithSixDashEnd` - Asserts `parse()` accepts real nsc output with five-dash BEGIN and six-dash END markers, extracting both JWT and seed (regression for the five-dash-only regex).
- `testFromFileParsesRealNscFixtureWhenPresent` - Asserts `fromFile()` on the real `build/nats/jwt/user.creds` fixture yields a JWT starting `ey` and a seed starting `S`; skips when the fixture is absent.
- `testParseRejectsMissingJwtBlock` - Asserts `parse()` throws NatsException ('NATS USER JWT block') when only the NKey seed block is present.
- `testParseRejectsMissingNkeySeedBlock` - Asserts `parse()` throws NatsException ('USER NKEY SEED block') when only the JWT block is present.
- `testFromFileRejectsNonExistentPath` - Asserts `fromFile()` throws NatsException ('not found or not readable') for a missing path.
- `testExtractBlockTreatsBlockTypeLiterallyOnBothMarkers` - Asserts the private `extractBlock()` matches the block type literally rather than as a regex: a type containing a metacharacter ('A.B') returns null when either the BEGIN or the END marker only regex-matches it ('AXB'), and returns the block body only when both markers carry the literal text.
- `testFromFileReadsValidCredsFile` - Asserts `fromFile()` reads a valid creds file written to a temp file, returning the expected jwt and nkeySeed values (cleaning up the temp file afterward).

### tests/Unit/DrainFlushBoundedWriteTest.php
- `testDrainCompletesWithinBudgetWhenWritesWedge` - Asserts a wedged drain-phase UNSUB write cannot hang drain(): the write wait is bounded by the drain budget, the flush phase is skipped, and the connection still reaches Closed well inside its ~requestTimeoutMs budget (the #149 write-phase twin, where cancelling the heartbeat first removed the only escalation out of the wedge).
- `testFlushTimesOutWhenPingWriteWedges` - Asserts a wedged flush() PING write fails inside the request-timeout budget with the documented TimeoutException naming backpressure, instead of parking forever before the read phase even starts, and that a following disconnect() still releases the wedged write and reaches Closed.
- `testDrainSkipsFlushPhaseWhenPingWriteWedges` - Asserts a wedged drain-phase flush PING (the first drain write when no subscriptions exist) surfaces as a cancellation through the PING write's own catch, skipping the flush phase, with drain still reaching Closed within its budget.
- `testDrainReportsDeadSocketPingWriteFailureAndStillCloses` - Asserts the disclosed #150 contract that drain() no longer throws on a dead-socket write failure: a flush-phase PING write that throws lets drain()->await() resolve, surfaces the TransportClosedException via the error listener rather than swallowing it, and still reaches Closed with the socket torn down.
- `testDrainReportsDeadSocketUnsubWriteFailureAndStillCloses` - Asserts the same #150 contract for the per-sid UNSUB writes: a dead-socket UNSUB write is reported through the error listener and drain still reaches Closed, instead of throwing out of drain() and stranding the connection in Draining with the socket open.
- `testDrainStillClosesWhenLoggerThrowsWhileReportingWriteFailure` - Asserts a user-supplied PSR logger that throws while the drain write failure is being reported cannot escape emitErrorSafely and break the teardown (#158): drain()->await() still resolves, the connection reaches Closed, and the transport is closed.
- `testPublishDuringDrainTimesOutWhenWriteWedges` - Asserts a handler ack published while drain delivers backlog has its write wait bounded like drain's own writes: the backlog message is still delivered, the PUB is attempted on the wire, the wedged publish fails with the documented TimeoutException ("Publish during drain timed out") instead of parking forever, and drain still reaches Closed.
- `testDrainBudgetBoundsSerialHandlerPublishesAcrossBacklog` - Asserts mid-drain handler publishes share drain()'s single budget rather than each re-arming a fresh requestTimeoutMs: with a wedged transport and six backlog messages acked twice each, drain completes in roughly one budget instead of the pre-fix ~K x 2 budgets, delivery stops at the first inter-message boundary past the deadline (only m1 and m2 arrive), the four dropped messages are reported loudly with their count, and the wedged acks fail into the handler with TimeoutException.
- `testDrainStillClosesWhenTransportCloseThrows` - Asserts drain()'s teardown closes the transport best-effort like every other terminal path: a throwing close() on the deliberately broken socket does not rethrow out of drain()->await(), the close attempt still runs, and the connection reaches Closed rather than being stranded in Draining (#150).
- `testDrainDeadlineWithOnlySuspendedHandlerReportsNoZeroCountDiscard` - Asserts a drain whose only remaining backlog is a dispatch suspended mid-handler, with nothing queued behind it, drops no message and therefore emits no "0 buffered message(s)" deadline-exceeded discard error (#149), while still reaching Closed past the suspended handler.

### tests/Unit/DrainScanPendingDirtyTest.php
- `testMessageToOneOfManyIdleSubscriptionsIsDelivered` - With 50 idle subscriptions, asserts a single MSG for one mid-list sid is still delivered to exactly that handler - the #162 dirty set routes the lone active sid without depending on scanning every live subscription.
- `testCrossSidDeliveryOrderMatchesRegistrationOrderNotWireOrder` - Feeds one chunk whose wire order is deliberately non-ascending (c1,a1,b1,c2,a2 across three sids) and asserts delivery is grouped by ascending-sid (registration) order with per-sid FIFO preserved (a1,a2,b1,c1,c2) - pinning that the #162 dirty-set drain keeps the old full-map scan's cross-sid ordering.
- `testDirtySetTracksOnlyActiveSidAndClearsWhenQueueEmpties` - With three idle subscriptions, enqueuing one message via `enqueueMessage()` puts ONLY that sid in `pendingDirty`; after `drainAllPending()` the message is delivered, the dirty entry is removed so it is not re-scanned (#162), and the emptied `SplQueue` is still retained for every sid (#139 no-realloc).
- `testHasUndeliveredDrainBacklogTracksDirtySet` - Asserts `hasUndeliveredDrainBacklog()` is false for an idle sid, true once a message is enqueued, and false again after the drain - staying consistent with the dirty set that `drain()`'s round-up loop (#149/#150) depends on.

### tests/Unit/ExceptionHierarchyTest.php
- `testNatsExceptionHierarchyImplementsMarker` - Asserts NatsException, ConnectionException, and JetStreamException are all instances of the NatsThrowable marker interface.
- `testTransportExceptionsImplementMarkerWhileRemainingRuntimeExceptions` - Asserts TransportClosedException and TlsRequiredException implement NatsThrowable and remain RuntimeExceptions, while deliberately NOT being NatsException instances.
- `testCatchNatsThrowableCatchesATransportException` - Asserts a thrown TransportClosedException is caught by a `catch (NatsThrowable)` block and the caught value is the TransportClosedException type.
- `testFallbackClientVersionIsCurrentSemver` - Asserts ProtocolCodec::FALLBACK_CLIENT_VERSION is a string matching semver `\d+.\d+.\d+` and is not the stale `1.0.1` placeholder.

### tests/Unit/FeatureSupportTest.php
- `testRequiredVersion` - Asserts the version registry returns the minimum NATS version for known fields (filter_subjects->2.10, allow_msg_ttl->2.11, allow_atomic->2.12) and null for unknown fields.
- `testUnsupportedFromApiErrorMapsKnownField` - Asserts an "unknown field" API error for a registered feature returns an UnsupportedFeatureException (also a JetStreamException) carrying feature, requiredVersion, serverVersion, code 400, and a message mentioning "requires NATS server 2.12+" and the server version.
- `testUnsupportedFromApiErrorIgnoresUnregisteredField` - Asserts an "unknown field" error for a field not in the registry returns null (treated as an ordinary error).
- `testUnsupportedFromApiErrorIgnoresOtherErrors` - Asserts a non-"unknown field" error (e.g. "stream not found", 404) returns null, not a feature-gap exception.
- `testUnsupportedFromApiErrorWithUnknownServerVersion` - Asserts a null server version produces an UnsupportedFeatureException with null serverVersion and a message containing "reports unknown".

### tests/Unit/JetStreamContextTest.php

- `testAccountInfo` - accountInfo() parses account metrics (memory/storage) and issues a `PUB $JS.API.INFO` request.
- `testAddStreamFromBuilder` - addStream() with a typed StreamConfiguration sends a STREAM.CREATE payload carrying subjects, retention, storage, max_bytes, max_age (ns), and num_replicas.
- `testAddConsumerFromBuilder` - addConsumer() with a typed ConsumerConfiguration sends a CONSUMER.CREATE payload with durable_name, ack_policy, max_deliver, ack_wait (ns), and backoff (ns).
- `testKeyValueBucketNames` - keyValueBucketNames() lists only KV_-prefixed streams with the prefix stripped.
- `testObjectStoreBucketNames` - objectStoreBucketNames() lists only OBJ_-prefixed streams with the prefix stripped.
- `testStreamNames` - streamNames() returns names via the STREAM.NAMES endpoint.
- `testConsumerNames` - consumerNames() returns names via the CONSUMER.NAMES.ORDERS endpoint.
- `testGetLastMessageForSubject` - getLastMessageForSubject() requests STREAM.MSG.GET with last_by_subj and parses the stored subject/payload.
- `testGetLastMessageForSubjectRejectsWildcard` - getLastMessageForSubject() throws JetStreamException ("non-wildcard") for a wildcard subject.
- `testCreateOrUpdateStreamFallsBackToUpdate` - createOrUpdateStream() retries with STREAM.UPDATE after a CREATE "already in use" error and returns the updated stream.
- `testApiErrorEnvelopeExposesErrCode` - a JetStream API error envelope's err_code (10059) is exposed via getErrCode() while getCode() keeps the HTTP-like 404 (#154).
- `testPublishExpectationMismatchExposesErrCode` - a publish-expectation error ack exposes err_code 10071 via getErrCode(); an envelope without err_code yields null (#154).
- `testCreateOrUpdateStreamFallsBackToUpdateByErrCode` - createOrUpdateStream() falls back to STREAM.UPDATE on err_code 10058 even when the description shares no wording with "already in use" (#154).
- `testCreateOrUpdateStreamRethrowsWhenErrCodeIsNotStreamNameInUse` - createOrUpdateStream() trusts a present err_code over a misleading "already in use" description (err_code 10065) and re-throws instead of updating (#154).
- `testCreateOrUpdateStreamFallsBackToUpdateWithoutErrCode` - createOrUpdateStream() still falls back to STREAM.UPDATE via the "already in use" substring when the envelope carries no err_code (old servers) (#154).
- `testStreamCrud` - createStream/getStream/deleteStream map to CREATE/INFO/DELETE endpoints and return expected name/subjects/success.
- `testJetStreamApiErrorMapping` - an API error payload on getStream() surfaces as a JetStreamException with the error description.
- `testJetStreamContextIsCached` - jetStream() returns the same cached JetStreamContext instance on repeated calls.
- `testObjectStoreConstructsEquivalentBucketPerCall` - objectStore() builds a fresh, equal-by-value ObjectStoreBucket per call (no memoization, nothing retained on the context) with the correct backing stream per bucket name (#133).
- `testKeyValueConstructsEquivalentBucketPerCall` - keyValue() builds a fresh, equal-by-value KeyValueBucket per call (no memoization, nothing retained on the context) with the correct backing stream per bucket name (#133).
- `testPullConsumerReturnsIterator` - pullConsumer() returns a PullConsumerIterator instance.
- `testConsumerCrud` - createConsumer/getConsumer/deleteConsumer map to CONSUMER CREATE/INFO/DELETE and return expected stream/name/success.
- `testCreateConsumerWithFilterSubjects` - createConsumer() with a filter_subjects array sends filter_subjects and omits the singular filter_subject.
- `testCreateConsumerRejectsBothFilterForms` - createConsumer() rejects supplying both a single filter and filter_subjects before dispatch (no extra writes).
- `testCreateConsumerRejectsEmptyFilterSubjectEntry` - createConsumer() rejects a filter_subjects array containing an empty string before dispatch.
- `testCreateConsumerRejectsFilterSubjectInOptionsConflict` - createConsumer() rejects filter_subject and filter_subjects both supplied via the options bag before dispatch.
- `testCreateEphemeralConsumerRejectsEmptyFilterSubject` - createEphemeralConsumer() rejects an empty filter subject ("must not be empty") before dispatch.
- `testCreatePushConsumerWithFilterSubjects` - createPushConsumer() forwards filter_subjects and omits the singular filter_subject.
- `testCreateConsumerWithPriorityGroups` - createConsumer() forwards priority_groups and priority_policy in the create payload.
- `testCreateConsumerRejectsInvalidPriorityPolicy` - createConsumer() rejects an unknown priority_policy ("must be one of") before dispatch.
- `testFetchBatchWithPullOptions` - fetchBatch() with pull options sends group, min_pending, max_bytes, and no_wait in the pull request.
- `testFetchBatchRejectsInvalidPriority` - fetchBatch() rejects an out-of-range priority (must be 0..9) before dispatch.
- `testFetchBatchRejectsUnknownPullField` - fetchBatch() with an unknown $pull key throws JetStreamException naming the offending key and the supported field set before anything reaches the wire, instead of silently dropping it (#132).
- `testFetchBatchSendsIdleHeartbeat` - fetchBatch() with idle_heartbeat (ADR-13, nanoseconds) carries the field into the pull request JSON on the wire (#132).
- `testFetchBatchRejectsIdleHeartbeatAboveHalfOfExpires` - fetchBatch() rejects an idle_heartbeat above 50% of expires client-side with a clear InvalidArgumentException before anything reaches the wire (ADR-13) (#153).
- `testFetchBatchRejectsNonPositiveIdleHeartbeat` - fetchBatch() rejects a non-positive idle_heartbeat ("must be a positive integer") before dispatch (#153).
- `testFetchBatchSurvivesSingleChunkBurstAboveThePendingCap` - the fetch reply inbox is slow-consumer-exempt: a 1300-delivery burst arriving in ONE read chunk (above the 1024 default per-subscription pending cap) reaches the caller whole and in order, where DropOldest previously discarded the head of a batch the server had already counted as delivered (#118/#120 twin).
- `testFetchBatchAcceptsIdleHeartbeatAtExactlyHalfOfExpires` - the ADR-13 boundary: an idle_heartbeat of exactly 50% of expires is accepted and reaches the wire (#153).
- `testFetchBatchFailsFastOnMissedIdleHeartbeats` - with idle_heartbeat requested and a silent transport (no message, no status-100 frame), fetchBatch() fails within ~2 heartbeat intervals (monotonic elapsed bound, well under the expires+slack deadline) with a "missed idle heartbeats" JetStreamException (nats.go ErrNoHeartbeat parity) (#153).
- `testUnpinConsumer` - unpinConsumer() issues a CONSUMER.UNPIN request carrying the group and returns true.
- `testPinIdOf` - pinIdOf() extracts the Nats-Pin-Id header value, returning null when absent.
- `testDirectGetBatchCollectsUntilEob` - directGetBatch() collects multiple HMSG replies, stops at the 204 EOB, and does not consume a frame sent after EOB.
- `testDirectGetBatchSurvivesSingleChunkBurstAboveThePendingCap` - the Direct Get batch inbox is slow-consumer-exempt: an 1100-reply burst in one read chunk is returned whole instead of a truncated prefix presented as complete, since Direct Get replies are never redelivered while the 204 EOB still arrives (#118/#120 twin).
- `testDirectGetLastForSubjectsTreatsAllMiss404AsEmpty` - a multi_last chunk whose subjects all have no stored message is answered with a lone 404 and contributes zero messages, so an all-absent subject set returns an empty list instead of aborting the whole call with an exception (ADR-31).
- `testDirectGetLastForSubjects` - directGetLastForSubjects() sends multi_last with batch sized to the subject count and terminates on Nats-Num-Pending: 0.
- `testDirectGetLastForSubjectsChunksAboveResultCap` - directGetLastForSubjects() splits an exact-subject list past the 1000-subject server result cap into two batched requests (1000 + 1, asserting batch sizes and the split boundary) and concatenates the replies in chunk order; the single-request pre-fix sent one oversized multi_last a 2.11+ server rejects with "Too Many Results" (#110).
- `testDirectGetLastForSubjectsChunksToRespectMaxPayload` - directGetLastForSubjects() also splits so no batched request's JSON payload exceeds the negotiated max_payload, with every input subject appearing in exactly one chunk; the pre-fix single request would exceed max_payload and be rejected by publish() (#110).
- `testDirectGetLastForSubjectsChunkPayloadAccountsForJsonSlashEscaping` - the per-chunk max_payload budget counts each subject with the same json_encode escaping the request is serialized with, so slash-bearing subjects (KV keys legally contain '/', which json_encode widens to '\/') do not overflow a chunk; a strlen-based estimate under-counts every slash and the oversized chunk's PUB is rejected by publish() (#110).
- `testDirectGetLastForSubjectsUsesDefaultBudgetWhenServerAdvertisesNoMaxPayload` - directGetLastForSubjects() falls back to the 1 MiB NATS default budget when the server advertises no max_payload (maxPayload() 0), packing ten short subjects into ONE batched request; collapsing the budget onto 0 would send one request per subject (#110).
- `testDirectGetLastForSubjectsChunkBoundaryPacksToExactPayloadBudget` - the per-chunk packing is byte-exact: with a max_payload that fits exactly two 9-byte subjects, three subjects split [2,1] (asserting each chunk's multi_last and batch); an off-by-one in the fill comparison (`>` vs `>=`) or an inflated per-subject cost would split [1,1,1] (#110).
- `testDirectGetLastForSubjectsChunkBoundaryCountsEverySubjectByte` - one byte tighter (budget below two subjects' cost), even two 9-byte subjects no longer share a chunk, splitting [1,1,1]; a phantom free byte in the byte accounting (initial/flush-reset currentBytes or an under-counted per-subject cost) would let two share a chunk (#110).
- `testDirectGetBatchSurfacesError` - directGetBatch() surfaces a 408 status frame as a JetStreamException with code 408.
- `testSupportsBatchedDirectGetGatesOnServerVersion` - supportsBatchedDirectGet() returns true only for NATS >= 2.11 (2.11/2.12/3.0, including a pre-release tag) and false for 2.10 or an unparseable version, the gate KV getAll()/Object Store list() use to choose the batched multi_last path vs the per-subject fan-out (#110).
- `testPublishWithAck` - publish() returns the stream/seq/duplicate ack and issues a `PUB orders.created` request.
- `testPublishWrapsMalformedAckAsJetStreamException` - publish() wraps a non-JSON ack as JetStreamException ("Malformed JetStream publish ack").
- `testPublishMapsApiError` - publish() maps an API error ack to a JetStreamException with the description.
- `testCreateStreamWithOptions` - createStream() forwards extra config options (allow_msg_schedules) into the CREATE payload.
- `testUnsupportedFeatureRaisesTypedExceptionWithServerVersion` - a version-gated field rejected by an old server surfaces as UnsupportedFeatureException carrying feature, requiredVersion (2.12), serverVersion (from INFO), code 400, and is a JetStreamException subclass.
- `testPublishScheduled` - publishScheduled() with Schedule::at() sends an HPUB with Nats-Schedule (@at RFC3339), Nats-Schedule-Target, and Nats-Schedule-TTL headers and returns the ack.
- `testPublishScheduledRejectsUnsupportedPattern` - publishScheduled() rejects a malformed schedule string ("Unsupported schedule expression") before dispatch.
- `testPublishScheduledEveryWithSourceAndRollup` - publishScheduled() with Schedule::every() emits @every plus Target, Source, and Rollup:sub headers and no time-zone header.
- `testPublishScheduledCronWithTimeZone` - publishScheduled() with Schedule::cron() emits the cron expression plus the Nats-Schedule-Time-Zone header.
- `testPublishScheduledPredefinedAlias` - publishScheduled() with Schedule::predefined('daily') emits @daily and may carry a time-zone header.
- `testPublishScheduledAtWithTimezoneOffset` - publishScheduled() with an @at string carrying a numeric RFC3339 offset reaches the wire unchanged.
- `testPublishScheduledRejectsTimeZoneForNonCron` - publishScheduled() rejects a time zone supplied for a non-cron schedule before dispatch.
- `testPublishWithMsgId` - publish() with msgId emits the Nats-Msg-Id header via HPUB and reflects ack.duplicate=true.
- `testPublishWithExpectationHeaders` - publish() emits optimistic-concurrency headers (Expected-Stream, Last-Sequence, Last-Subject-Sequence, Last-Msg-Id) via HPUB.
- `testPublishExpectationMismatchThrows` - a precondition-mismatch error ack ("wrong last sequence") surfaces as a JetStreamException and is not retried.
- `testPublishRetriesOnNoResponders` - publish() retries after a transient 503 no-responders frame and succeeds on the retry, returning the ack seq.
- `testAckSyncSendsAckAsRequestAndAwaitsConfirmation` - ackSync() sends +ACK as a request (SUB on fresh inbox + PUB with reply inbox) and resolves on the empty confirmation reply.
- `testDeleteMessageFastAndSecure` - deleteMessage() sends no_erase=true by default and omits no_erase for secureErase, with the correct seq in each MSG.DELETE request.
- `testMessageMetadataParsesAckTuple` - messageMetadata() parses both the 9-token and domain-qualified 11-token $JS.ACK forms (stream, consumer, delivered, sequences, pending, domain, timestamp).
- `testMessageMetadataThrowsForNonJetStreamMessage` - messageMetadata() throws JetStreamException ("not a JetStream delivery") for a non-$JS.ACK reply.
- `testPublishWithTtlSeconds` - publish() with an integer ttl emits Nats-TTL in seconds via HPUB.
- `testPublishWithTtlNever` - publish() with ttl 'never' passes the Nats-TTL:never header through unchanged.
- `testPublishRejectsZeroTtl` - publish() rejects a zero/sub-second TTL ("at least 1 second") before dispatch.
- `testPublishRejectsEmptyMsgId` - publish() rejects an empty msgId ("Nats-Msg-Id must not be empty") before dispatch.
- `testIncrementCounter` - incrementCounter() emits the Nats-Incr header via HPUB and returns the new value string.
- `testIncrementCounterPreservesBigValue` - incrementCounter() preserves a counter value beyond PHP_INT_MAX as an exact string (JSON_BIGINT_AS_STRING).
- `testIncrementCounterRejectsMalformedDelta` - incrementCounter() rejects a non-integer delta ("must be an integer string") before dispatch.
- `testCounterValue` - counterValue() reads the latest value via a Direct Get (`PUB $JS.API.DIRECT.GET.COUNTERS`) and returns the val.
- `testCounterValueMissingReturnsZero` - counterValue() returns "0" when the Direct Get reports 404 message-not-found.
- `testPublishScheduledOmitsTtlWhenNotProvided` - publishScheduled() omits the Nats-Schedule-TTL header when ttl is null.
- `testPublishScheduledMapsApiError` - publishScheduled() maps an API error ack ("scheduler down") to a JetStreamException.
- `testFetchNext` - fetchNext() uses the CONSUMER.MSG.NEXT endpoint with expires in ns and returns the delivered payload.
- `testFetchNextRejectsInvalidExpiresMs` - fetchNext() rejects a zero expiresMs ("must be greater than zero").
- `testAckHelpersPublishProtocolTokens` - ack/nak/nakWithDelay/term/inProgress publish +ACK, -NAK, -NAK {delay}, +TERM, +WPI respectively to the message reply subject.
- `testNakWithDelayRejectsInvalidDelay` - nakWithDelay() rejects a zero delayMs ("requires delayMs greater than zero").
- `testAckRequiresReplySubject` - ack() throws JetStreamException ("requires a reply subject") for a message with no reply subject.
- `testCreatePushConsumer` - createPushConsumer() sets deliver_subject and ack_policy explicit, marks the result as push, and uses CONSUMER.CREATE.ORDERS.PROC.
- `testCreateEphemeralPushConsumer` - createEphemeralPushConsumer() sends deliver_subject and omits durable_name.
- `testSubscribePushConsumerHandlesFlowControl` - a push subscription auto-replies to a status-100 flow-control HMSG (PUB to the reply subject) and forwards the subsequent real payload to the handler.
- `testSubscribePushConsumerAnswersStalledHeartbeat` - a stalled idle-heartbeat (Nats-Consumer-Stalled header, no reply) is answered on the Nats-Consumer-Stalled subject and not delivered to the handler.
- `testSubscribePushConsumerIgnoresHeartbeat` - a plain idle-heartbeat control message is ignored (no handler call, no PUB to its reply subject).
- `testCreateEphemeralConsumer` - createEphemeralConsumer() uses the stream-level CREATE endpoint (no durable suffix), sends ack_policy explicit and filter_subject, omits durable_name.
- `testSubscribeEphemeralPushConsumer` - subscribeEphemeralPushConsumer() creates the consumer with deliver_subject (no durable_name) and forwards the delivered payload to the handler.
- `testCreateStreamRejectsEmptySubjects` - createStream() rejects empty subjects unless a mirror is provided.
- `testCreateStreamAllowsSourcesWithoutSubjects` - createStream() allows empty subjects when a non-empty sources config is provided, sending sources and an empty subjects array for a pure aggregate stream that ingests only from sources.
- `testCreateStreamAllowsMirrorWithoutSubjects` - createStream() allows empty subjects when a mirror config is provided, sending mirror and empty subjects in the payload.
- `testCreateConsumerRejectsEmptyFilterSubject` - createConsumer() rejects an empty filter subject ("must not be empty").
- `testRequestJsonWrapsJsonException` - a malformed (non-JSON) API response surfaces as JetStreamException ("Malformed JetStream API response").
- `testUpdateStream` - updateStream() uses the STREAM.UPDATE endpoint and returns the updated name/subjects.
- `testCreateConsumerWithOptions` - createConsumer() forwards ack_policy, max_deliver, ack_wait, max_ack_pending, and filter_subject options into the payload.
- `testCreateConsumerDefaultsAckPolicyToExplicit` - createConsumer() defaults ack_policy to explicit when none is supplied on the durable path.
- `testCreatePushConsumerAllowsAckPolicyOverride` - createPushConsumer() honors an explicit ack_policy override (none) in the payload.
- `testFetchBatch` - fetchBatch() pulls a batch with batch/expires fields set and returns the collected message payloads in order.
- `testFetchBatchRejectsInvalidBatch` - fetchBatch() rejects a zero batch size ("must be greater than zero").
- `testFetchBatchIgnoresTerminalStatusFrames` - fetchBatch() returns the received message(s) and ignores a trailing 404 terminal status frame.
- `testFetchBatchSurfacesMidBatchTerminalStatusToCallback` - fetchBatch() returns the partial batch and surfaces a mid-batch 409 terminal status (code+description) to the onTerminalStatus callback.
- `testFetchBatchIgnoresStatus100ControlFrames` - fetchBatch() ignores a leading status-100 idle-heartbeat control frame and still returns the real message.
- `testFetchBatchThrowsWhenNoMessagesArrive` - fetchBatch() throws JetStreamException ("ended with status 404: No Messages") when only a 404 status arrives.
- `testFetchBatchThrowsTerminalStatusDescription` - fetchBatch() throws a JetStreamException with code 409 and the description for a 409 MaxAckPending terminal status.
- `testPauseConsumerSendsCorrectPayload` - pauseConsumer() sends CONSUMER.PAUSE with the pause_until timestamp and returns the paused result.
- `testResumeConsumerSendsEmptyBody` - resumeConsumer() uses the CONSUMER.PAUSE endpoint and returns paused=false.
- `testSubscribeOrderedConsumerSendsCorrectConfig` - subscribeOrderedConsumer() creates a consumer with flow_control, idle_heartbeat, ack_policy none, mem_storage, and num_replicas 1 (ADR-17 parity, #132) set.
- `testPurgeStream` - purgeStream() uses STREAM.PURGE and returns the purged count.
- `testPurgeStreamWithSubjectFilter` - purgeStream() with a filter option includes the filter in the purge payload.
- `testListStreams` - listStreams() uses STREAM.LIST and returns parsed StreamInfo objects.
- `testListStreamsWithSubjectFilter` - listStreams() with a subject option includes the subject filter in the request.
- `testListConsumers` - listConsumers() uses CONSUMER.LIST and returns ConsumerInfo objects with push flag derived from deliver_subject.
- `testListStreamsPaginatesAcrossPages` - listStreams() paginates across pages (offset 0 then 2) and returns all streams from both pages.
- `testGetStreamMessage` - getStreamMessage() uses STREAM.MSG.GET with seq and base64-decodes the stored subject/data.
- `testExtractStreamSequenceParsesReplySubject` - (reflection) extractStreamSequence() parses the stream sequence from a 9-token $JS.ACK reply subject.
- `testExtractStreamSequenceParsesDomainQualifiedReplySubject` - (reflection) extractStreamSequence() parses the stream sequence from a 12-token domain-qualified $JS.ACK subject.
- `testExtractStreamSequenceParses13TokenReplySubject` - (reflection) extractStreamSequence() parses the stream sequence (index 7) from a 13-token $JS.ACK subject - offsets anchor from the front and trailing tokens are ignored, nats.go parity (#155).
- `testKeyValueRejectsInvalidBucketName` - keyValue() rejects a dotted bucket name ("Invalid bucket name").
- `testObjectStoreRejectsInvalidBucketName` - objectStore() rejects a slashed bucket name ("Invalid bucket name").
- `testExtractSequencesParseElevenTokenDomainReplySubject` - (reflection) extractStreamSequence() parses the stream sequence from an 11-token domain $JS.ACK subject without a trailing random token.
- `testExtractStreamSequenceReturnsNullForInvalidReplySubject` - (reflection) extractStreamSequence() returns null for no-reply, too-short, wrong-prefix, and non-integer-sequence subjects.
- `testHandlePushControlMessageInterceptsNon100StatusButNotDataMessages` - (reflection) handlePushControlMessage() returns false for a status-0 data message with user headers but true for a non-100 status frame (404), so error/control frames are withheld from the handler while data still flows (#121).
- `testHandlePushControlMessageHeartbeatWithoutReplyReturnsTrue` - (reflection) handlePushControlMessage() returns true for a status-100 heartbeat lacking a reply subject.
- `testHandlePushControlMessageRepliesToJetStreamFlowControlSubject` - (reflection) handlePushControlMessage() returns true and PUBs an empty reply to the $JS.FC flow-control reply subject.
- `testGetStreamMessagePreservesZeroPayload` - getStreamMessage() preserves a falsy "0" body and leaves rawHeaders null.
- `testGetStreamMessageDecodesHeaders` - getStreamMessage() base64-decodes the stored hdrs block onto rawHeaders and the body onto payload.
- `testGetStreamMessageWithoutHeadersReturnsNullRawHeaders` - getStreamMessage() leaves rawHeaders null when no header block is stored.
- `testDirectGetStreamMessageReturnsRawBodyAndHeaders` - directGetStreamMessage() returns the original subject (Nats-Subject), raw body, and decoded headers via the DIRECT.GET endpoint.
- `testDirectGetLastMessageForSubjectRequestsLastBySubj` - directGetLastMessageForSubject() requests DIRECT.GET with last_by_subj and returns subject/payload.
- `testDirectGetStreamMessageThrowsOnNotFound` - directGetStreamMessage() throws JetStreamException ("Message Not Found") on a 404 status reply.
- `testSubscribeOrderedConsumerRecreatesOnSequenceGap` - subscribeOrderedConsumer() discards an out-of-order delivery, deletes the old consumer once, recreates exactly once from opt_start_seq (last in-order+1), and delivers only the in-order message.
- `testStopOrderedConsumerStopsAfterRecreateRotatedTheSid` - stopOrderedConsumer() resolves the CURRENT sid through the shared state, so a stop issued with the ORIGINAL sid after a gap-driven rotation still unsubscribes the rotated sid and deletes the current server-side consumer, which a plain unsubscribe() can no longer reach.
- `testPushConsumerHeartbeatSequenceMismatchIsSurfaced` - an idle heartbeat whose Nats-Last-Consumer runs ahead of the locally tracked sequence surfaces a consumer sequence mismatch through the error listener once per gap episode: a matching heartbeat does not false-positive, a repeat of the same persistent gap does not spam a second report, and a delivery advancing the local sequence re-arms the signal for a later episode (ADR-9).
- `testPushConsumerHeartbeatMismatchNotSignaledBeforeFirstSessionDelivery` - the heartbeat gap check arms only once a delivery fixes a session baseline, so re-attaching to a durable whose first heartbeat reports historical Nats-Last-Consumer does not false-alarm (nats.go checkForSequenceMismatch parity).
- `testPushConsumerHeartbeatRegressionSignalsReplacementAndRebasesTracker` - a heartbeat reporting Nats-Last-Consumer below the session's tracked maximum is reported once as a server-side consumer replacement, rebases the tracker to the reported value, and re-arms gap detection so a real gap in the new instance fires against its own numbering (ADR-9).
- `testPushConsumerDeliveryRegressionRebasesTrackerSilently` - a delivered consumer sequence below the session's tracked maximum rebases the tracker SILENTLY while still reaching the handler, so a later heartbeat measures gaps against the new instance rather than the stale high-water mark (ADR-9).
- `testStopOrderedConsumerDuringInFlightRecreateDoesNotResurrect` - a stop racing an in-flight recreate is honored: when a later create attempt succeeds after the stop, the recreate sees the stopped latch and tears the fresh instance down (unsubscribe plus delete) instead of installing it or re-arming the watchdog, so no consumer keeps delivering after a successful stop.
- `testStopRacingRecreateWhoseAttemptsExhaustEmitsNoTerminalError` - the other arm of the stop/recreate race: a stopped consumer whose racing recreate then exhausts every attempt with the connection still Open releases the never-adopted fresh inbox, deregisters the ordered-stop handle, and surfaces NO terminal recreate-failed error for a consumer the operator deliberately stopped.
- `testSubscribeOrderedConsumerDeliversReplayArrivingDuringRecreateCreate` - a replayed frame that arrives on the rotated deliver inbox DURING the recreate CONSUMER.CREATE's read-pump (before its reply) is still delivered: the new instance is adopted (name + expected-sequence reset) before the create await, so the frame is not name-filtered out and no gap-storm results (#122 regression; only the pre-gap message was delivered under load).
- `testSubscribeOrderedConsumerIgnoresStaleDeliveryFromPreviousConsumerInstance` - a stale delivery from a different consumer instance name is ignored (no recreate) even when its consumer sequence matches the expected next.
- `testSubscribeOrderedConsumerRecreatesOnHeartbeatTailGap` - an idle heartbeat reporting Nats-Last-Consumer ahead of what was processed triggers exactly one recreate from the last in-order point (opt_start_seq).
- `testTerminalRecreateFailureReachesTheLogger` - a terminally dead ordered consumer produces an error-level PSR-3 log line even when no errorListener is configured, so the death is never signalled on no channel at all.
- `testSubscribeOrderedConsumerContainsRecreateFailure` - a recreate whose every attempt fails during gap recovery is contained (does not escape the shared dispatch loop); only the in-order message is delivered and the terminal failure surfaces once via the error listener as a JetStreamException mentioning "after 3 attempts" (#114).
- `testSubscribeOrderedConsumerRecreateRetriesThroughTransientFailure` - a transient recreate failure during gap recovery is retried: the second create attempt succeeds as a new consumer instance, delivery resumes from it, and nothing reaches the error listener (#114).
- `testSubscribeOrderedConsumerRecreatesWhenDeleteConsumerTimesOut` - a deleteConsumer() TimeoutException during gap recovery is best-effort cleanup: control still reaches the create-retry loop, the consumer is recreated, delivery resumes from the new instance, and no terminal recreate failure is reported (#151).
- `testSubscribeOrderedConsumerRecreatesWhenDeleteConsumerFailsWithConnectionError` - a deleteConsumer() ConnectionException (fatal -ERR frame during the delete reply wait) during gap recovery is best-effort cleanup: the create-retry loop still runs, the consumer is recreated, delivery resumes, and no terminal recreate failure is reported (#151).
- `testSubscribeOrderedConsumerDeliversFilteredMessagesWithoutSpuriousRecreate` - consecutive consumer sequences with non-contiguous stream sequences (filtered consumer) are all delivered in order with no delete/recreate.
- `testCreateOrUpdateStreamRethrowsNonAlreadyInUseError` - createOrUpdateStream() re-throws a non-"already in use" CREATE error ("JetStream not enabled") instead of falling back to UPDATE.
- `testStreamNamesWithNullStreamsKeyReturnsEmpty` - streamNames() returns an empty list when the response has no streams key.
- `testDirectGetThrowsForUnrecognizedResponse` - directGet (via directGetLastMessageForSubject) throws ("unrecognized response") when the reply lacks Nats-Stream/Nats-Sequence headers.
- `testDirectGetLastForSubjectsWithEmptySubjectsReturnsEmpty` - directGetLastForSubjects() returns an empty list immediately for an empty subjects array.
- `testDirectGetLastForSubjectsRejectsWildcardSubjectWithStar` - directGetLastForSubjects() rejects a subject containing '*' ("expects exact subjects").
- `testDirectGetLastForSubjectsRejectsWildcardSubjectWithGreaterThan` - directGetLastForSubjects() rejects a subject containing '>' ("expects exact subjects").
- `testDirectGetBatchRejectsZeroExpiresMs` - directGetBatch() rejects a non-positive expiresMs ("must be greater than zero").
- `testAddOrUpdateConsumerDelegatesToCreateConsumer` - addOrUpdateConsumer() delegates to createConsumer(), producing the CONSUMER.CREATE.ORDERS.PROC wire payload.
- `testConsumerNamesWithMissingConsumersKeyReturnsEmpty` - consumerNames() returns an empty list when the response has no consumers key.
- `testSubscribeEphemeralPushConsumerIgnoresControlMessages` - subscribeEphemeralPushConsumer() absorbs a status-100 flow-control frame (PUBs the FC reply, does not forward) and delivers the subsequent real message.
- `testSubscribeOrderedConsumerIgnoresControlMessages` - subscribeOrderedConsumer() absorbs a status-100 idle-heartbeat without forwarding it to the handler.
- `testSubscribeOrderedConsumerDeliversMessageWithoutAckMetadata` - subscribeOrderedConsumer() best-effort delivers messages with an absent or plain (non-$JS.ACK) reply subject SILENTLY - the unparseable-ack error (#155) applies only to reply subjects claiming the ack form.
- `testSubscribeOrderedConsumerEmitsErrorForUnparseableAckSubject` - subscribeOrderedConsumer() surfaces an ack-form reply subject the parser cannot read (10 tokens) through the error listener ("unparseable $JS.ACK reply subject") while still delivering the message best-effort, without triggering a recreate (#155).
- `testSubscribeOrderedConsumerEmitsUnparseableAckErrorOncePerConsumer` - the unparseable-ack error fires once per consumer instance, not once per message: two unparseable deliveries produce two best-effort deliveries but a single error (#155).
- `testSubscribeOrderedConsumerUnparseableAckErrorRearmsAfterRecreate` - the once-per-instance unparseable-ack error latch re-arms on a recreate: an unparseable delivery on the post-recreate consumer epoch emits a second error (#155).
- `testSubscribeOrderedConsumerToleratesDeleteConsumerFailure` - subscribeOrderedConsumer() absorbs a deleteConsumer error during gap recovery, still recreates, and delivers only the in-order message.
- `testSubscribeOrderedConsumerRecreatesOnMissedHeartbeats` - the idle-heartbeat watchdog recreates an ordered consumer whose transport goes totally silent (no data, heartbeat, or flow-control) for more than two intervals: a second CONSUMER.CREATE reaches the wire with no error, and because nothing had been delivered yet the recovery re-applies the INITIAL deliver policy rather than a by_start_sequence replay from 1 (which would wrongly re-replay the whole stream for a 'new'/'last_per_subject' watch), detecting a reaped/lost consumer the sequence-gap logic never sees (#113).
- `testSubscribeOrderedConsumerHeartbeatsPreventFalsePositiveRecreate` - an ordered consumer that keeps receiving idle heartbeats every interval is never recreated (each frame rearms the watchdog), so a legitimately quiet-but-alive consumer is left untouched (#113).
- `testSubscribePushConsumerSurfacesErrorOnMissedHeartbeats` - a caller-owned durable push consumer created with idle_heartbeat whose transport goes silent surfaces exactly one ErrConsumerNotActive-style error ("not active") through the error listener and never deletes/recreates the consumer (#113).
- `testSubscribeOrderedConsumerWatchdogTimerIsCancelledOnUnsubscribe` - subscribing an ordered consumer arms exactly one EventLoop repeat timer and unsubscribing cancels it (probed via EventLoop identifiers), so the watchdog never outlives its subscription or leaks (#113, #126 parity).
- `testSubscribeOrderedConsumerWatchdogAndGapRecreateDoNotRunConcurrently` - a gap delivery fed while the watchdog's recreate is parked on its delete await (the two paths run on different fibers, only the dispatch one covered by the per-sid guard) triggers a second recreate; an in-flight guard suppresses it so ORD1 is deleted exactly once - without the guard it is deleted twice and a second consumer is orphaned (#113).
- `testSubscribeOrderedConsumerWatchdogRefiresAfterSuccessfulRecreate` - a successful watchdog recreate (ORD1 -> ORD2) clears the miss latch and rebases the silence clock, so a still-silent ORD2 is itself recreated (a second DELETE.ORD2 on the wire); without the clear the watchdog stays wedged after the first recreate (#113).
- `testSubscribeOrderedConsumerWatchdogSurvivesTransientReconnect` - with the connection forced into Connecting (reflection), the watchdog rebases its clock and neither recreates nor cancels its timer past the miss threshold, so it survives a transient reconnect; pins the ordering of the Closed / isSubscriptionActive / not-Open checks (#113).
- `testWatchdogRecreateDeferredByReconnectRefiresOnceOpenAgain` - a watchdog-triggered recreate that dies fail-fast while the connection is not Open takes the deferral branch, clears the miss latch, and lets the watchdog re-fire once the connection is Open again, re-establishing the consumer with a second CONSUMER.CREATE and no error.
- `testWatchdogRecreateDeferredAfterAdoptedInboxRefiresOnceOpenAgain` - the other deferral arm, where the connection leaves Open after the fresh deliver inbox was subscribed and the candidate name adopted: the deferral releases the adopted-but-never-confirmed fresh inbox, rewinds to the pre-episode instance, and the post-reconnect watchdog re-fires against that same instance, silently.
- `testPlainUnsubscribeReleasesOrderedStopRegistryAndDeletesConsumer` - the watchdog's self-cancel tick runs the onDefunct hook when an ordered consumer is stopped with a plain unsubscribe(), releasing the orderedStops entry that otherwise leaked the stop closure, watchdog state, and user handler forever, and best-effort deleting the current server-side consumer.
- `testRotatedOutWatchdogTimerDoesNotReleaseStopRegistryOrDeleteConsumer` - only the CURRENT watchdog timer may run the onDefunct cleanup: a rotated-out timer whose sid is dead by design still self-cancels but neither releases the stop registry nor deletes the consumer living on the new sid.
- `testDeferralRewindsAdoptionSoSurvivorFramesDeliverAfterReconnect` - the disconnect-collision deferral rewinds the adopt-before-await dispatch state (consumer name and expected sequence), so when the episode's delete never took effect server-side the surviving old consumer's next in-order frame passes the name filter and delivers immediately after reconnect instead of stalling until the first idle heartbeat.
- `testWatchdogRecreateDeferralLatchesNotOpenObservationAcrossReapAwaits` - the deferral decision is LATCHED rather than re-sampled after the orphan reap's event-loop yields, so a reconnect completing in that window cannot turn a genuine disconnect collision into a loud terminal teardown: no error surfaces and a fresh watchdog episode re-establishes the consumer.
- `testUnpinConsumerRejectsEmptyGroup` - unpinConsumer() rejects an empty priority group name ("must not be empty").
- `testPublishWithLastSubjectSequenceHeaderMismatchThrowsImmediately` - publish() with expectedLastSubjectSequence (HPUB path) immediately re-throws a precondition-mismatch JetStreamException without retrying.
- `testCounterValueRethrowsNon404Exception` - counterValue() re-throws a non-404 (403) JetStreamException from the Direct Get instead of returning "0".
- `testIncrementCounterWithMalformedResponsePayload` - incrementCounter() wraps a non-JSON counter response as JetStreamException ("Malformed counter response").
- `testIncrementCounterWithApiErrorInResponse` - incrementCounter() maps an embedded API error in the counter response to a JetStreamException.
- `testIncrementCounterWithIntegerValField` - incrementCounter() returns a string for an unquoted integer val field.
- `testIncrementCounterWithMissingValFieldThrows` - incrementCounter() throws ("did not include a value") when the counter response has no val field.
- `testFetchBatchThrowsTimeoutWhenNoMessagesAndNoTerminalStatus` - fetchBatch() throws a 408 JetStreamException ("No messages received within timeout") when no messages and no terminal status arrive.
- `testAckSyncThrowsForEmptyReplySubject` - ackSync() throws JetStreamException ("requires a reply subject") when replyTo is an empty string.
- `testCreateConsumerRejectsNonArrayFilterSubjects` - createConsumer() rejects a non-array filter_subjects ("must be a non-empty array of subjects") before dispatch.
- `testCreateConsumerRejectsEmptyArrayFilterSubjects` - createConsumer() rejects an empty-array filter_subjects ("must be a non-empty array of subjects") before dispatch.
- `testFetchBatchRejectsInvalidPullGroupName` - fetchBatch() rejects an invalid pull group name ("1..16 characters of [A-Za-z0-9-_/=]") before dispatch.
- `testCreateConsumerRejectsEmptyPriorityGroups` - createConsumer() rejects an empty priority_groups array ("must be a non-empty array of group names") before dispatch.
- `testCreateConsumerRejectsInvalidPriorityGroupName` - createConsumer() rejects an invalid priority group name ("names must be 1..16 characters...") before dispatch.
- `testDirectGetBatchThrowsWhenDeadlineFiresBeforeCompletion` - directGetBatch() throws a JetStreamException (not a silent empty result) when the wait-cancellation fires on a blocking idle socket before any end-of-batch marker arrives (#121).
- `testJsRequestRethrowsNonNoRespondersNatsException` - jsRequest() (via incrementCounter) re-throws a non-"No responders" NatsException (a TimeoutException) unchanged.
- `testPublishWithRetryRethrowsWhenRetriesExhausted` - publishWithRetry() re-throws a 503 JetStreamException ("No JetStream responder") after all configured retry attempts are exhausted on transient no-responder failures.
- `testCreateStreamRejectsDottedStreamNameBeforeDispatch` - createStream() rejects a dotted stream name ("Invalid stream name") before any $JS.API write reaches the wire (#131).
- `testCreateConsumerRejectsDottedConsumerName` - createConsumer() rejects a dotted consumer name before dispatch, which the server would otherwise misroute as the filtered-create form (#131).
- `testGetConsumerRejectsDottedConsumerName` - getConsumer() rejects a dotted consumer name before dispatch instead of surfacing a misleading 503 from a non-existent API route (#131).
- `testDeleteConsumerRejectsDottedConsumerName` - deleteConsumer() rejects a dotted consumer name before dispatch (#131).
- `testDirectGetStreamMessageRejectsDottedStreamName` - directGetStreamMessage() rejects a dotted stream name before dispatch, closing the silent sibling-stream Direct Get read (#131).
- `testCreateStreamRejectsEmptyStreamName` - createStream() rejects an empty stream name ("must be non-empty") before dispatch (#131).
- `testGetStreamRejectsWildcardStreamName` - getStream() rejects a wildcard '*' stream name before dispatch (#131).
- `testCreateStreamAcceptsValidNameWithHyphenAndUnderscore` - createStream() accepts a valid name containing '-' and '_' (ORDERS-2_prod) and produces the expected STREAM.CREATE subject on the wire (#131).
- `testSubscribeOrderedConsumerReapsOrphanFromLostCreateReply` - during gap recovery, a first create attempt whose reply is lost (a -ERR raised as ConnectionException) leaves an orphan; the retry adopts a new consumer and a DELETE for the first attempt's client-chosen name reaches the wire while the adopted name is not deleted, so the orphan is best-effort reaped in addition to the rotation's self-heal (#122).
- `testSubscribeOrderedConsumerIgnoresOrphanHeartbeatOnRotatedOldInbox` - after a recreate rotates the deliver inbox, an orphan's PLAIN idle heartbeat (no FC subject) arriving on the OLD, now-unsubscribed inbox does not trigger a further recreate (still exactly one DELETE and two CREATEs, no error) - the falsifiable core of the fix, since before rotation that heartbeat drove a recreate storm (#122).
- `testSubscribeOrderedConsumerRotatesDeliverInboxAndUnsubscribesOld` - a recreate subscribes a fresh deliver inbox (a distinct _INBOX.JS.ORD subject / new sid), points the recreate CONSUMER.CREATE at that rotated deliver_subject, and unsubscribes the previous inbox (UNSUB 2), so an orphan on the old inbox becomes unreachable (#122).
- `testSubscribeOrderedConsumerTearsDownSubscriptionOnTerminalRecreateFailure` - a terminal recreate failure surfaces the error once and unsubscribes BOTH the rotated and the original deliver inbox (UNSUB 2), so a late frame on the torn-down original inbox is not delivered - "dead" is actually dead (#122).
- `testSubscribeEphemeralPushConsumerDropsNon100StatusFrames` - non-100 status control frames (409/503/408) on the deliver subject are intercepted and never forwarded to the user handler as data; with no error listener configured the drop is silent (#121).
- `testCallerOwnedPushConsumerSurfacesTerminalStatusViaErrorListener` - a caller-owned push consumer that receives a terminal 4xx/5xx status frame (409 Consumer Deleted) surfaces it through the error listener as a descriptive JetStreamException (code 409, mentioning the status and description) instead of silently dropping it, while a status-100 idle heartbeat stays intercepted-and-silent; neither reaches the handler as data (#121).
- `testDurableCallerOwnedPushConsumerSurfacesStatus400` - the DURABLE caller-owned push path surfaces a terminal status through the error listener exactly like the ephemeral path, pinning the full message (consumer, stream, status plus description, resubscribe remedy) and the terminal boundary at exactly status 400 (#121).
- `testPublishRejectsAckWithoutStream` - publish() throws a JetStreamException (mentioning "stream") when the ack carries neither an error nor a stream, instead of returning a bogus PubAck('', 0) success (#121).
- `testDirectGetBatchThrowsOnTimeoutBeforeCompletion` - directGetBatch() throws a JetStreamException rather than returning a truncated prefix when one data frame arrives but no 204 EOB / Nats-Num-Pending:0 completes the batch and the progress-based stall interval elapses with no further frame (#121).
- `testUpdateStreamRejectsDottedStreamName` - updateStream() rejects a dotted stream name before dispatch (only CONNECT+PING written), pinning the name guard on the STREAM.UPDATE path (#131).
- `testAddStreamRejectsDottedStreamName` - addStream() rejects a StreamConfiguration whose name is dotted before dispatch, pinning the builder-path name guard (#131).
- `testDeleteStreamRejectsDottedStreamName` - deleteStream() rejects a dotted stream name before dispatch (#131).
- `testPurgeStreamRejectsDottedStreamName` - purgeStream() rejects a dotted stream name before dispatch (#131).
- `testListConsumersRejectsDottedStreamName` - listConsumers() rejects a dotted stream name before dispatch (#131).
- `testConsumerNamesRejectsDottedStreamName` - consumerNames() rejects a dotted stream name before dispatch (#131).
- `testGetStreamMessageRejectsDottedStreamName` - getStreamMessage() rejects a dotted stream name before dispatch (#131).
- `testGetLastMessageForSubjectRejectsDottedStreamName` - getLastMessageForSubject() rejects a dotted stream name before dispatch (#131).
- `testDeleteMessageRejectsDottedStreamName` - deleteMessage() rejects a dotted stream name before dispatch (#131).
- `testDirectGetLastMessageForSubjectRejectsDottedStreamName` - directGetLastMessageForSubject() rejects a dotted stream name before dispatch (#131).
- `testDirectGetBatchRejectsDottedStreamName` - directGetBatch() rejects a dotted stream name before dispatch (#131).
- `testCreateConsumerRejectsDottedStreamName` - createConsumer() rejects a dotted STREAM name before dispatch, pinning the stream guard independently of the consumer-name guard (#131).
- `testCreateEphemeralConsumerRejectsDottedStreamName` - createEphemeralConsumer() rejects a dotted stream name before dispatch (#131).
- `testGetConsumerRejectsDottedStreamName` - getConsumer() rejects a dotted STREAM name before dispatch, pinning the stream guard independently of the consumer-name guard (#131).
- `testPullConsumerRejectsDottedStreamName` - pullConsumer() rejects a dotted STREAM name synchronously before building the iterator (#131).
- `testPullConsumerRejectsDottedConsumerName` - pullConsumer() rejects a dotted CONSUMER name synchronously (valid stream), pinning the second name guard in isolation (#131).
- `testAddConsumerRejectsDottedConsumerName` - addConsumer() rejects a dotted CONSUMER name from the config before dispatch (valid stream), pinning the consumer guard in isolation (#131).
- `testCreatePushConsumerRejectsDottedConsumerName` - createPushConsumer() rejects a dotted CONSUMER name before dispatch (valid stream) (#131).
- `testPauseConsumerRejectsDottedConsumerName` - pauseConsumer() rejects a dotted CONSUMER name before dispatch (valid stream) (#131).
- `testResumeConsumerRejectsDottedConsumerName` - resumeConsumer() rejects a dotted CONSUMER name before dispatch (valid stream) (#131).
- `testUnpinConsumerRejectsDottedConsumerName` - unpinConsumer() rejects a dotted CONSUMER name before dispatch (valid stream), ahead of the empty-group check (#131).
- `testFetchBatchRejectsDottedConsumerName` - fetchBatch() rejects a dotted CONSUMER name before dispatch (valid stream) (#131).
- `testDirectGetRethrowsNonNoRespondersNatsExceptionUnchanged` - directGet() maps only the no-responders NatsException to the catchable 503 Direct Get unavailable error and passes every other NatsException through unchanged, so a silent responder surfaces the raw TimeoutException instead of a 503 that would misdirect the caller into the leader-path fallback.
- `testDirectGetLastForSubjectsFailsLoudlyWhenMaxPayloadIsBelowTheBatchEnvelope` - a max_payload smaller than the batched Direct Get envelope clamps the payload budget to its floor and the resulting oversized single-subject chunk is rejected LOUDLY by publish()'s max_payload guard (no silent hang, infinite split loop, or dropped subjects), with the failed chunk's reply inbox still released (#110).
- `testSubscribeEphemeralPushConsumerInvokesOnConsumerCreatedBeforeSubscribing` - subscribeEphemeralPushConsumer()'s onConsumerCreated hook receives the CREATE response's parsed ConsumerInfo (num_pending included) BEFORE the deliver subscription is established, so a caller can arm an end-of-initial-data signal (#99).
- `testSubscribeOrderedConsumerContainsThrowingOnConsumerCreatedHook` - a throwing onConsumerCreated hook on an ordered consumer is contained and surfaced through the error listener while the subscription still goes live and delivers.
- `testSubscribeOrderedConsumerCoalescesGapTriggerDuringInFlightRecreate` - the recreate closure's re-entrancy guard: a second gap trigger dispatched while a recreate is already in flight no-ops, so exactly one DELETE plus CREATE pair reaches the wire for the whole episode and the replay is still delivered (#113).
- `testStopDuringInFlightRecreateToleratesTeardownFailures` - the stopped-teardown arms of a stop-raced recreate are best-effort: with the fresh inbox's UNSUB write failing and the fresh instance's CONSUMER.DELETE rejected, local subscription state is still released, nothing is resurrected, no watchdog is re-armed, and no terminal error surfaces.
- `testSubscribeOrderedConsumerRecoverySurvivesOldInboxUnsubscribeFailure` - a failed UNSUB of the OLD deliver inbox at the end of a successful recreate is best-effort and does not undo the recovery: the rotated consumer stays installed and keeps delivering both the mid-recreate replay and a later delivery, with no error and no recreate storm (#122).
- `testSubscribeOrderedConsumerTerminalTeardownSurvivesUnsubscribeFailures` - the terminal teardown's inbox unsubscribes are best-effort: with BOTH the fresh and the old inbox UNSUB writes failing at the transport, the terminal error still surfaces once, both inboxes are released locally so a late frame is dropped, and nothing escapes the dispatch loop (#122).
- `testSubscribeOrderedConsumerRecreatesOnTerminalStatusControlFrame` - a non-100 terminal status control frame on the ordered deliver inbox (409 Consumer Deleted) is withheld from the user handler and triggers a recreate from the last in-order point, so delivery resumes with the replacement instead of waiting forever for pushes that cannot come (#121).
- `testStopOrderedConsumerToleratesUnsubscribeAndDeleteFailures` - stopOrderedConsumer()'s teardown is best-effort end to end: with the live inbox's UNSUB write failing and the server rejecting the CONSUMER.DELETE, the stop still resolves cleanly with the watchdog cancelled, local subscription state dropped, and no error surfaced.
- `testStopOrderedConsumerFallsBackToPlainUnsubscribeForUnregisteredSid` - stopOrderedConsumer() falls back to a plain unsubscribe for a sid with no registered ordered consumer, rather than failing or silently no-opping.
- `testFetchBatchReturnsPartialBatchOnMissedIdleHeartbeats` - a heartbeat miss after at least one delivery returns the PARTIAL batch once two silent intervals elapse, instead of discarding real messages with a throw or sitting out the full expires+slack deadline, leaving the heartbeat-miss exception to the empty-fetch case (#153).
- `testCallerOwnedPushConsumerDoesNotSurfaceSub400StatusFrames` - a non-100 but sub-400 status control frame is withheld from the user handler like every control frame yet is not terminal, so no error is surfaced for it and only 4xx/5xx statuses report.
- `testThrowingLoggerOnSurfacedPushStatusDoesNotBreakDispatch` - JetStreamContext's emitClientError() wrapper swallows a throwing user logger, so a logger blowing up on a surfaced push status cannot break the shared dispatch loop and the next delivery still reaches the handler (#150 twin).
- `testClientOptionsAccessorReturnsConstructedOptionsInstance` - NatsClient::options() hands back the exact NatsOptions instance the client was constructed with, so components wired off the client observe the same runtime configuration object rather than a copy.
- `testInvalidNameRejectionCarriesTheCompleteForbiddenCharacterList` - the invalid-name rejection message names the kind, echoes the offending name, and enumerates every forbidden character class, pinned in full so no clause can be dropped, and fires before any write reaches the wire (#131).
- `testAddConsumerRejectsDottedStreamName` - addConsumer() rejects a dotted STREAM name before dispatch (#131).
- `testAddOrUpdateConsumerRejectsDottedStreamName` - addOrUpdateConsumer() rejects a dotted stream name before dispatch (#131).
- `testAddOrUpdateConsumerRejectsDottedConsumerName` - addOrUpdateConsumer() rejects a dotted consumer name before dispatch (#131).
- `testCreatePushConsumerRejectsDottedStreamName` - createPushConsumer() rejects a dotted STREAM name before dispatch (#131).
- `testSubscribePushConsumerRejectsDottedStreamName` - subscribePushConsumer() rejects a dotted stream name before dispatch (#131).
- `testSubscribePushConsumerRejectsDottedConsumerName` - subscribePushConsumer() rejects a dotted consumer name before dispatch (#131).
- `testSubscribeEphemeralPushConsumerRejectsDottedStreamName` - subscribeEphemeralPushConsumer() rejects a dotted stream name before dispatch (#131).
- `testCounterValueRejectsDottedStreamName` - counterValue() rejects a dotted stream name before dispatch (#131).
- `testFetchNextRejectsDottedStreamName` - fetchNext() rejects a dotted stream name before dispatch (#131).
- `testFetchNextRejectsDottedConsumerName` - fetchNext() rejects a dotted consumer name before dispatch (#131).
- `testFetchBatchRejectsDottedStreamName` - fetchBatch() rejects a dotted STREAM name before dispatch (#131).
- `testDirectGetLastForSubjectsKeepsLaterChunksWhenAnEarlierChunkIs404` - a 404 no-results reply for chunk 1 of a chunked multi_last contributes zero messages while chunk 2's results are kept, so an all-absent chunk cannot throw away the rest of the enumeration (ADR-31, #110).
- `testDefaultBudgetPacksTwoSubjectsFillingItExactlyIntoOneChunk` - the 1 MiB fallback payload budget is byte-exact at its upper edge: two subjects encoding to exactly the 1048512-byte budget share ONE batched request, where a budget one byte smaller would spill the second into its own request (#110).
- `testDefaultBudgetSplitsTwoSubjectsOneByteOverItIntoTwoChunks` - the same fallback budget is byte-exact at its lower edge: two subjects encoding to one byte over 1048512 split into two requests, where a budget one byte larger would pack them into a single over-budget request (#110).
- `testUnpinConsumerCoercesNumericSuccessToBool` - unpinConsumer() coerces a non-boolean success field strictly, returning boolean false for an explicit 0 rather than reading it as true or raising a TypeError.
- `testPublishRetryClampsNegativeRetryWaitToZero` - a misconfigured NEGATIVE publishRetryWaitMs clamps to zero and retries immediately instead of breaking the retry loop with an invalid sleep, with the retried publish still resolving to the ack.
- `testPublishRetryActuallyWaitsBetweenAttempts` - the retry wait is a real pause: with publishRetryWaitMs 40 the retried publish cannot complete before roughly 40ms have elapsed, so a recovering JetStream API is not hammered with an immediate retry.
- `testFetchBatchRejectsIdleHeartbeatJustAboveHalfOfExpires` - the ADR-13 idle_heartbeat <= expires/2 gate is exact on the expires side: a heartbeat 400ns above half of a 1000ms expiry is rejected with the precise nanosecond figures before any wire traffic (#153).
- `testFetchBatchHeartbeatMissWindowIsExactlyTwoIntervals` - the pull heartbeat-miss deadline is exactly two idle_heartbeat intervals and the thrown message reports that window in ms, so the miss fires ahead of the expires+slack deadline instead of degrading into the generic 408 timeout (#153).
- `testPushConsumerZeroConsumerSeqTokenDoesNotRebaseTracker` - a delivery whose $JS.ACK consumer-sequence token casts to 0 does not rebase the delivered-sequence tracker, so the next idle heartbeat does not false-alarm a sequence mismatch for deliveries this client actually processed (ADR-9).
- `testPushConsumerSteadyTrafficNeverTripsTheWatchdog` - every inbound frame rearms the push watchdog, so a consumer receiving steady traffic is never reported not active however the frame timing aligns with the watchdog ticks (#113).
- `testLateWatchdogTickAfterUnsubscribeDoesNotReportAStall` - a watchdog tick that finds its guarded subscription gone stops at the teardown (self-cancel plus defunct cleanup) and never falls through into the miss check, so even a late tick with more than two silent intervals on the clock reports no stall for an already-unsubscribed consumer (#113).
- `testPushConsumerWatchdogArmsOnlyForPositiveIntegerIdleHeartbeat` - only a POSITIVE INTEGER idle_heartbeat arms the watchdog: a zero interval and a numeric string both register no repeat timer while the subscription still succeeds (#113).
- `testSubscribeEphemeralPushConsumerArmsWatchdogForIdleHeartbeat` - an ephemeral push subscription with idle_heartbeat set arms exactly one repeat watchdog timer, built from the shared watchdog state (#113).
- `testNon100StatusFrameWithFlowControlReplyIsNotAcked` - a non-100 status frame carrying a $JS.FC reply subject is withheld WITHOUT being acked as flow control, and instead surfaces as a terminal status for the caller-owned consumer.
- `testOrderedConsumerTerminalStatusWithLastConsumerHeaderRecreatesExactlyOnce` - a terminal status control frame that also carries Nats-Last-Consumer triggers exactly ONE recreate because the handler returns instead of falling through into the heartbeat tail-gap check, which would cascade a second spurious recreate of the just-recreated consumer.
- `testOrderedConsumerZeroStreamSeqTokenDoesNotResetResumeCursor` - the by_start_sequence resume cursor ignores a malformed stream-sequence token that casts to 0, so a later recreate restarts from the last GENUINE in-order stream sequence + 1 rather than re-applying the initial deliver policy.
- `testRecreateTriggeredDuringStopTeardownDoesNotRecreate` - a recreate trigger that fires while stopOrderedConsumer() is still mid-teardown observes the stop latch and does nothing: no delete of the already-stopped instance, no fresh inbox, no CONSUMER.CREATE.
- `testDeferralKeepsAdoptionWhenCandidateDeliveredSoSurvivorReplayIsNotDuplicated` - the deferral rewind is skipped when the episode already delivered: an adopted candidate that replayed a message keeps its adopted state, so the surviving old consumer's post-reconnect frame for an already-seen stream sequence is name-filtered rather than re-delivered as a duplicate, while the never-installed fresh inbox is still released.
- `testDefunctTickDuringParkedRecreateCreatePreventsInstall` - onDefunct latches the stopped flag FIRST, so a plain unsubscribe whose defunct tick runs while a recreate is parked in its CONSUMER.CREATE await makes the resumed recreate tear the fresh instance down (unsubscribe the rotated inbox, best-effort delete the created consumer) instead of installing it, leaving no watchdog re-armed and no zombie consumer delivering.

### tests/Unit/JsMessageMetadataTest.php
- `testFromMessageReturnsNullWhenReplyToIsNull` - `fromMessage()` returns null when the message has no reply subject.
- `testFromMessageReturnsNullWhenFirstTokenIsNotJs` - `fromMessage()` returns null when the first reply-subject token is not "$JS".
- `testFromMessageReturnsNullWhenSecondTokenIsNotAck` - `fromMessage()` returns null when the second token is not "ACK" (e.g. "NAK").
- `testFromMessageReturnsNullForUnrecognisedTokenCount` - `fromMessage()` returns null for a reply subject whose token count matches neither the exact 9-token v1 form nor the >= 11-token v2 form (7 tokens).
- `testFromMessageParses9TokenForm` - Parses a canonical 9-token `$JS.ACK` subject and asserts stream, consumer, numDelivered=3, streamSequence=42, consumerSequence=7, timestampNanos, numPending=5, and domain=null.
- `testFromMessageParses11TokenFormWithRealDomain` - Parses the 11-token domain-prefixed subject and asserts domain="hub" plus all numeric/stream/consumer fields including numPending=0.
- `testFromMessageNormalizesUnderscoreDomainToNull` - When the domain token is "_" (server placeholder), the parsed `domain` is normalized to null.
- `testFromMessageParses12TokenForm` - Parses the 12-token form (domain + trailing random token), silently ignoring the 12th token while all other fields parse correctly.
- `testFromMessageParses13TokenForm` - Parses a 13-token subject (a future server form beyond the known 12 tokens): offsets 2..10 anchor from the front and parse identically, extras ignored - nats.go tolerant-parser parity (#155).
- `testFromMessageParses14TokenForm` - Parses a 14-token subject: any number of trailing tokens beyond index 10 is ignored (#155).
- `testFromMessageReturnsNullFor10TokenForm` - A 10-token subject (between the 9-token v1 form and the >= 11-token v2 form) is rejected; the trailing-token tolerance starts at 11 (#155).
- `testTimestampReturnsCorrectUtcDatetime` - `timestamp()` converts a nanosecond epoch to a `DateTimeImmutable` with zero UTC offset and the expected "2023-11-14 22:13:20" date/time.
- `testTimestampPreservesMicrosecondPrecision` - `timestamp()` preserves sub-second precision, formatting 500_000 ns as microseconds "000500".
- `testTimestampHandlesZeroNanoseconds` - `timestamp()` returns a `DateTimeImmutable` of "1970-01-01 00:00:00" for a zero-nanosecond value.

### tests/Unit/KeyValueBucketTest.php

- `testGetFallsBackToStreamMessageWhenDirectGetUnavailable` - when Direct Get returns 503 (no-responders), get() falls back to STREAM.MSG.GET and still returns the value (operation PUT, revision 9), emitting both DIRECT.GET and STREAM.MSG.GET requests.
- `testBucketCreateAndDelete` - create() and deleteBucket() map to STREAM.CREATE.KV_cfg and STREAM.DELETE.KV_cfg, returning the created stream name and a true delete result.
- `testBucketCreateSendsKvDefaultsAndAllowsOverride` - create() defaults include deny_delete:true and discard:new (ADR-8/nats.go parity) and an explicit user override (deny_delete:false, discard:old) wins (#132).
- `testPutGetDelete` - put/get/delete round-trip parses values correctly and uses the right subjects (PUB for put, DIRECT.GET for get, HPUB with KV-Operation:DEL for delete).
- `testDeleteHeaderRequestNormalizesNoRespondersTo503` - A no-responders reply to a KV header request (delete/update/purge via publishWithHeadersAck) surfaces as JetStreamException(503) ("No JetStream responder"), not a bare NatsException, matching jsRequest()'s taxonomy (#161).
- `testCreateKeySucceedsWhenAbsent` - createKey() on an absent key publishes with Nats-Expected-Last-Subject-Sequence:0 and returns the ack (#19).
- `testCreateKeyThrowsWhenKeyExists` - createKey() throws JetStreamException "Key already exists" when the wrong-last-sequence ack is followed by a get() showing a live value (#19).
- `testCreateKeyDetectsWrongLastSequenceByErrCode` - createKey() detects the wrong-last-sequence rejection by err_code 10071 even when the description shares no wording with "wrong last sequence" (#154).
- `testCreateKeyDetectsWrongLastSequenceWithoutErrCode` - createKey() still detects wrong-last-sequence via the description substring when the envelope carries no err_code (old servers) (#154).
- `testCreateKeyRethrowsWhenErrCodeIsNotWrongLastSequence` - An envelope whose err_code is present but not 10071 is rethrown as-is even when the description misleadingly says "wrong last sequence" - a present err_code wins over wording (#154).
- `testCreateKeyExistsExceptionCarriesBothCodes` - the "Key already exists" collision exception carries the HTTP-like 400 in getCode() and the API err_code 10071 in getErrCode() (#154).
- `testCreateWithMirrorTranslatesBucketName` - create() with a mirror translates the mirror bucket name to KV_src, emits an empty subjects list, and targets STREAM.CREATE.KV_dst (#62).
- `testCreateWithSourcesAndExtendedConfig` - create() with sources translates each source name to KV_b1/KV_b2, attaches the mandatory ADR-57 transform re-subjecting each source's records into this bucket's prefix, keeps the bucket's own subjects (only mirrors omit them), and passes through compression and placement config (#62).
- `testCreateSourceWithCustomTransformsPassesThroughVerbatim` - a source carrying its own subject_transforms passes through verbatim (no KV_ name prefixing, no auto transform), which is how a non-KV stream is sourced into a bucket (nats.go/ADR-57 full-control rule).
- `testCreateMirrorEnablesMirrorDirectAndWritesThroughToOrigin` - a mirror bucket create sets mirror_direct:true and the handle's writes go through to the ORIGIN's subject ($KV.src.theme, never the mirror's own $KV.dst.theme), which a mirror stream ingesting no subjects of its own would otherwise answer with 503.
- `testCreateCrossDomainMirrorRoutesWritesAndReadsThroughOrigin` - a cross-domain mirror (the `domain` shorthand) converts to the external api $JS.HUB.API (the shorthand itself never reaches the wire), routes writes through `<api>.$KV.src.<key>`, and reads the LOCAL mirror stream under the origin's subject.
- `testBindResolvesMirrorPrefixesFromStreamInfo` - bind() resolves the mirror routing from STREAM.INFO for a handle attached to a mirror bucket created elsewhere, after which its writes go through to the origin exactly as on a created handle.
- `testFailedMirrorCreateLeavesHandleOnPlainBucketSubjects` - a REJECTED mirror create leaves the handle on its own bucket subjects (prefixes apply only after a successful create), so puts publish to $KV.dst.theme rather than through the foreign API and gets read this bucket's own subject.
- `testRebindToNonMirrorStreamClearsStalePrefixes` - re-binding a handle to a stream that is no longer a mirror clears the stale mirror prefixes, so only the pre-rebind put targets the origin and later writes use the bucket's own subject.
- `testCreateSourceBucketAliasIsKvPrefixedUnconditionally` - the `bucket` source alias is KV_-prefixed unconditionally, so a bucket named "KV_x" resolves to stream KV_KV_x with a transform on $KV.KV_x.>, not to bucket x's stream and subjects.
- `testCreateSourceNameKeyKeepsKvStreamNameSemantics` - the `name` source key keeps nats.go stream-name semantics: "KV_x" already names the backing stream, is used as-is (never double-prefixed), and its transform reads bucket x's subjects.
- `testCreateMirrorBucketAliasIsKvPrefixedUnconditionally` - the `bucket` mirror alias is KV_-prefixed unconditionally too: mirroring bucket "KV_x" targets stream KV_KV_x and the write-through prefix resolves to $KV.KV_x.<key>, not bucket x's.
- `testCreateMirrorNameKeyKeepsKvStreamNameSemantics` - a mirror `name` that is already the backing KV_ stream name is used as-is and never double-prefixed.
- `testGetRevisionReturnsEntryAtSequence` - getRevision() reads a specific sequence via STREAM.MSG.GET (with "seq":2) and returns the entry at revision 2 (#33).
- `testGetRevisionReturnsNullForDifferentKey` - getRevision() returns null when the message at that sequence belongs to a different key (#33).
- `testDeleteWithExpectedRevisionSendsHeader` - delete() with an expected revision emits the KV-Operation:DEL marker plus Nats-Expected-Last-Subject-Sequence:4 compare-and-delete header (#34).
- `testHistoryReturnsEmptyWhenNoPending` - history() returns an empty array when the consumer reports num_pending:0 (#41).
- `testHistoryCollectsAllRevisions` - history() collects all delivered revisions in order (values v1,v2; revisions 5,6), stopping when pending reaches zero (#41).
- `testHistoryToleratesDeliveryWithoutMetadataAndKeepsCollecting` - history() skips a metadata-less delivery (no $JS.ACK reply subject) without throwing out of the dispatch loop and records only the valid revision (#96).
- `testKeysReturnsLiveKeyNames` - keys() enumerates BOTH live key names via a last_per_subject + headers_only ephemeral consumer filtered on the bucket keyspace (prefix + '>'), excluding a DEL tombstone by the KV-Operation header and an out-of-prefix frame (null key), never issues a Direct Get, and unsubscribes the deliver subscription when done (#25/#110).
- `testKeysReturnsEmptyWithoutSubscribingWhenConsumerReportsNoPending` - keys() short-circuits to [] WITHOUT subscribing to the deliver inbox when the consumer reports num_pending 0; a seeded live frame would be enumerated if it wrongly proceeded past the guard (#110).
- `testKeysTreatsAbsentNumPendingAsNoLiveKeys` - keys() reads an ABSENT num_pending as 0 (the `?? 0` default) and short-circuits to [] without subscribing (#110).
- `testKeysCastsFloatZeroNumPendingToNoLiveKeys` - keys() coerces a JSON float 0.0 num_pending with (int) before the `=== 0` test, still short-circuiting to [] (#110).
- `testKeysThrowsOnStalledReplayAndStillUnsubscribes` - keys() fails with a JetStreamException bounded by the CALLER-supplied progress timeout (quoted in the message) when the replay makes no progress, and still tears down the deliver subscription on the throw path (#110/#121).
- `testWatchOptionsConfigureConsumer` - watch() with includeHistory/metaOnly/ignoreDeletes drives deliver_policy:all and headers_only:true on the consumer config (#26).
- `testWatchResumeFromRevisionUsesStartSequence` - watch() with resumeFromRevision:42 configures deliver_policy:by_start_sequence and opt_start_seq:42 (#26).
- `testWatchSilentConsumerIsRecreatedByWatchdog` - a silent KV watch consumer (which must actually request the idle heartbeat it is tuned with) is deleted and recreated by the ordered-consumer watchdog instead of hanging forever, and since nothing was delivered before the stall the replacement re-applies the watch's INITIAL deliver policy rather than a by_start_sequence replay from sequence 1; recreation is recovery, so no error is surfaced (#113).
- `testWatchRequestsDefaultIdleHeartbeat` - an option-less KV watch still requests the default idle heartbeat (KeyValueBucket::WATCH_IDLE_HEARTBEAT_NS) so the watchdog protects the common watch form too (#113).
- `testWatchDetectsDeliveryGapAndRecreatesFromLastRevision` - a watch that MISSES a delivery (the consumer sequence skips) discards the out-of-order entry, deletes the gapped consumer and recreates it exactly once with deliver_policy:by_start_sequence at the last delivered revision + 1, so the missed range is replayed instead of silently lost.
- `testGetAllFallsBackToStreamMessageWhenDirectGetUnavailable` - getAll() on a bucket without Direct Get falls back per key to the leader STREAM.MSG.GET read and still returns the values, instead of failing the whole enumeration on the 503.
- `testGetAllFallsBackWhenBatchedDirectGetAnswers503` - on a 2.11+ server whose bucket answers the batched multi_last Direct Get with 503, getAll() falls through to the per-subject fan-out (with its leader read) and still returns the values.
- `testGetMissingReturnsNull` - get() returns null when Direct Get replies with a 404 not-found status.
- `testInvalidKeyRejected` - put() with a key containing a space throws JetStreamException "Invalid KV key".
- `testUpdateWithExpectedRevision` - update() sends the optimistic Nats-Expected-Last-Subject-Sequence:2 header and returns the new ack sequence.
- `testPurge` - purge() emits KV-Operation:PURGE and Nats-Rollup:sub headers and returns the ack sequence.
- `testPutWithTtl` - put() with a per-key ttl emits the Nats-TTL:60s header (#4).
- `testDeleteWithTombstoneTtl` - delete() with a tombstoneTtl emits Nats-TTL:120s alongside the KV-Operation:DEL marker (#4).
- `testGetStatus` - getStatus() maps stream state counters to bucket/stream/messages/bytes fields.
- `testGetAll` - getAll() on a pre-2.11 server returns only the latest non-deleted values per key, skipping a PURGE-marked key, using the concurrent per-subject Direct Get fan-out fallback (#110 gate routes 2.10 here).
- `testGetTreatsMarkerAsTombstone` - get() treats a server delete-marker (Nats-Marker-Reason) as a PURGE tombstone with a null value rather than a live empty string (#5).
- `testGetAllOmitsMarker` - getAll() omits a key whose latest record is a server delete-marker, returning only the live key (#5).
- `testWatchTreatsMarkerAsTombstone` - watch() delivers a server delete-marker as a PURGE tombstone (null value), not a live empty value (#5).
- `testCreateWithSubjectDeleteMarkerTtl` - create() forwards subject_delete_marker_ttl into the KV stream config (#5 passthrough).
- `testPutAcceptsAdr8KeyCharset` - put() accepts a key using the full ADR-8 charset (dots, hyphens, underscores, equals, slashes, mixed case, digits) and returns the ack.
- `testPutRejectsKeyOutsideAdr8Charset` - put() rejects keys outside the ADR-8 charset ('@', '#', non-ASCII, ':') with "Invalid KV key" and nothing beyond CONNECT+PING reaches the wire (#132).
- `testPutRejectsReservedKvPrefixKey` - put() rejects a key with the reserved "_kv" prefix (ADR-8) with a message naming the reservation (#132).
- `testPutRejectsKeyWithWildcard` - put() with a key containing '*' throws JetStreamException "Invalid KV key".
- `testPutRejectsKeyWithLeadingTrailingOrConsecutiveDots` - put() rejects keys with leading, trailing, or consecutive dots (.theme, theme., a..b), each throwing "Invalid KV key".
- `testPutRejectsKeyWithTab` - put() with a key containing a tab character throws JetStreamException "Invalid KV key".
- `testCreateWithSemanticOptions` - create() maps semantic options (history->max_msgs_per_subject, ttl->max_age, max_value_size->max_msg_size, storage, num_replicas) into the stream config.
- `testWatchDispatchesEntries` - watch() over a push consumer dispatches a delivered update to the callback as a KeyValueEntry (key/value/revision-from-$JS.ACK), returns the subscription sid, and sets deliver_policy:new, ack_policy:none, and an inactive_threshold.
- `testGetPropagatesNon404ApiErrors` - get() propagates a non-404 Direct Get error (500) as a JetStreamException.
- `testDeleteWrapsMalformedReplyAsJetStreamException` - delete() wraps a non-JSON ack as a JetStreamException "Malformed JetStream reply" rather than a raw JsonException.
- `testGetMapsDeleteMarkerToNullValue` - get() with a KV-Operation:DEL marker maps to operation DEL with a null value and the correct revision.
- `testBucketNameHelpers` - streamName() returns "KV_cfg" and subjectPrefix() returns "$KV.cfg.".
- `testUpdateRejectsNonPositiveExpectedRevision` - update() with expected revision 0 throws JetStreamException "Expected revision must be greater than zero".
- `testGetAllSkipsKeysThatReturnNotFound` - getAll() skips a key whose Direct Get races a deletion and returns 404, returning only the surviving key.
- `testGetAllThrowsOnStreamInfoApiError` - getAll() surfaces a STREAM.INFO API error ("stream not found") instead of swallowing it into an empty result.
- `testGetStatusFallsBackLastSequenceToMessagesWhenMissing` - getStatus() falls back last_sequence to the messages count when last_seq is absent from state.
- `testDeletePropagatesApiError` - delete() propagates a JetStream API error ("delete failed") as a JetStreamException.
- `testCreateWithMirrorArrayBucketKeyTranslatesName` - create() with a mirror given as an array with a 'bucket' key replaces it with name:KV_src, retains extra fields (start_seq), drops the 'bucket' key, and emits empty subjects (#62).
- `testCreateWithSourcesArrayBucketKeyTranslatesNames` - create() with sources given as arrays with 'bucket' keys translates each to name:KV_alpha/KV_beta and removes the 'bucket' key (#62).
- `testPurgeWithTombstoneTtlAndExpectedRevision` - purge() with both a tombstone TTL and an expected revision emits KV-Operation:PURGE, Nats-TTL:300s, and Nats-Expected-Last-Subject-Sequence:6.
- `testGetRevisionThrowsOnNonPositiveRevision` - getRevision() with revision 0 throws JetStreamException "Revision must be greater than zero".
- `testGetRevisionReturnsNullOnNotFound` - getRevision() returns null when STREAM.MSG.GET replies with a 404 error.
- `testGetRevisionPropagatesNon404Error` - getRevision() re-throws a non-404 STREAM.MSG.GET error ("internal server error").
- `testGetFallbackReturnsNullOnStreamMessage404` - the STREAM.MSG.GET fallback (after a 503 Direct Get) returns null when the API replies with a 404 error.
- `testGetFallbackPropagatesNon404StreamMessageError` - the STREAM.MSG.GET fallback propagates a non-404 API error ("service unavailable").
- `testGetFallbackReturnsNullWhenMessageFieldMissing` - the STREAM.MSG.GET fallback returns null when the reply JSON has no 'message' field.
- `testGetFallbackDecodesEncodedHeaders` - the STREAM.MSG.GET fallback decodes base64 'hdrs' and resolves a KV-Operation:DEL header to operation DEL with a null value at revision 11.
- `testGetFallbackThrowsOnMalformedBase64Data` - the STREAM.MSG.GET fallback throws JetStreamException "Malformed KV payload for key theme" when message.data is invalid base64.
- `testWatchIgnoresMessagesOnNonKvSubject` - watch() silently skips a delivery whose subject does not match the KV bucket prefix (keyFromSubject returns null), leaving the callback uninvoked.
- `testCreateKeyRethrowsNonWrongLastSequenceError` - createKey() re-throws a publish error that is not the wrong-last-sequence code (err_code 10000, "internal error").
- `testCreateKeySucceedsAfterKeyDeletedEntryIsNull` - createKey() retries with expected-seq 0 when after a wrong-last-sequence error the get() returns 404 (null entry), and succeeds.
- `testCreateKeySucceedsAfterTombstoneRevision` - createKey() retries against the tombstone's revision (expected-seq 5) when get() returns a DEL tombstone at seq 5, and succeeds with the new ack.
- `testCreateWithDescriptionAndMaxBytesOptions` - create() passes through 'description' and 'max_bytes' options into the stream config.
- `testPutRejectsEmptyKey` - put() with an empty key throws JetStreamException "Invalid KV key".
- `testPutRejectsKeyWithGreaterThan` - put() with a key containing '>' throws JetStreamException "Invalid KV key".
- `testGetAllReturnsEmptyWhenNoSubjects` - getAll() returns an empty array immediately when STREAM.INFO reports no subjects.
- `testGetAllPropagatesNon404DirectGetError` - getAll() propagates a non-404 (500) Direct Get error as a JetStreamException.
- `testCreateKeyWithTtlPassesTtlHeader` - createKey() with a ttl emits both the Nats-Expected-Last-Subject-Sequence:0 CAS header and Nats-TTL:3600s.
- `testGetAllSkipsSubjectsWithNonKvPrefix` - getAll() skips STREAM.INFO subjects not matching the bucket KV prefix (keyFromSubject null) and returns only matching keys.
- `testGetAllUsesSingleBatchedDirectGetOnModernServer` - getAll() on a 2.11+ server issues exactly ONE batched multi_last Direct Get carrying every key subject (asserted count 1, batch 3), returns BOTH live keys (map not truncated to one), excludes a DEL tombstone, and skips a leading non-key (bare-prefix) subject while still collecting the keys after it; the single-request assertion fails on the pre-#110 per-subject fan-out (#110).
- `testWatchUpdatesOnlyUsesNewDeliverPolicy` - watch() with updatesOnly:true uses deliver_policy:new.
- `testWatchWithDefaultOptionsUsesLastPerSubject` - watch() with a default KeyWatchOptions() instance uses deliver_policy:last_per_subject.
- `testWatchWithOnCaughtUpToleratesMessageWithoutMetadata` - watch() with an onCaughtUp callback does not throw when a delivery lacks $JS.ACK metadata; the entry still reaches the handler and caughtUp stays false since it can't be determined (#90).
- `testWatchFiresOnCaughtUpImmediatelyOnEmptyBucket` - watch() on an empty bucket (num_pending:0) fires the onCaughtUp signal immediately from the created consumer's num_pending, without any delivery (#99).
- `testHistoryThrowsWhenDeadlineFiresBeforeCaughtUp` - history() throws a JetStreamException (not a truncated prefix) when the replay makes NO progress for the whole progress-timeout interval before num_pending reaches 0 (one revision then silence); the per-call progress-timeout argument exercises the path deterministically (#121).
- `testHistoryDoesNotThrowWhileReplayKeepsMakingProgress` - history()'s bound is progress-based, not a whole-replay deadline: revisions fed 0.1 s apart (each gap under the 0.5 s bound) with a total replay (~0.6 s) exceeding the bound complete without throwing, in order, since each revision resets the stall clock - a whole-replay deadline would have thrown here (#121).
- `testCreateSourceBucketAliasWithCustomTransformsStillKvPrefixesName` - the `bucket` alias is still translated to the backing KV_ stream name inside the custom-subject_transforms pass-through branch, while the caller's transforms pass through verbatim, no auto transform is attached, and the client-side alias key never reaches the wire.
- `testCreateSourceWithKvPrefixedNameIsNotDoublePrefixed` - a source name that is ALREADY the backing KV_ stream name is used as-is, so the mandatory transform re-subjects from the origin bucket's $KV.<src>.> keyspace and never from a bogus $KV.KV_<src>.>.
- `testCreateRejectsMirrorWithBothDomainAndExternal` - a mirror entry carrying both `domain` and `external` is rejected client-side with "A KV mirror/source cannot set both domain and external", before any STREAM.CREATE reaches the server.
- `testCreateMirrorWithEmptyNameLeavesPrefixesUnchanged` - a mirror with an EMPTY name resolves to no origin, so the prefix re-resolution is a no-op and writes stay on this bucket's own $KV.dst.<key> rather than a broken "$KV.." subject.
- `testBindRejectsStreamThatIsNotAKvBucket` - bind() rejects a stream that is not KV-shaped (no max_msgs_per_subject >= 1) with 'Stream "KV_cfg" is not a KV bucket (max_msgs_per_subject < 1)', matching nats.go's ErrBadBucket.
- `testKeysToleratesDeliveryWithoutMetadataAndKeepsEnumerating` - keys() skips a metadata-less delivery (no $JS.ACK reply subject) even when its subject looks like a real key, enumerates only the well-formed record, and still unsubscribes the deliver subscription (the #90 class).
- `testHistoryStallsOnFullyIdleDeliverSubscriptionAndUnsubscribes` - on a FULLY idle deliver subscription history() enforces its bound by cancelling the in-flight socket read: it surfaces the stall error quoting the caller's 0.050 s bound and the 0 revisions collected, tears down the deliver subscription, and leaves no orphaned read (#121).
- `testGetAllFanOutPropagatesNon404Non503DirectGetError` - in the per-subject fan-out a Direct Get error that is neither 404 (skipped key) nor 503 (leader fallback) propagates verbatim (code 500 and description) and fails the enumeration, with no batched multi_last attempted.
- `testGetAllBatchedDropsReplyFramesOutsideBucketPrefix` - a batched Direct Get reply frame whose stored subject lies outside this bucket's keyspace is dropped from the result instead of surfacing under a garbage key.
- `testWatchOptionsRejectNonPositiveIdleHeartbeat` - KeyWatchOptions rejects a zero or negative idleHeartbeat at construction with InvalidArgumentException "idleHeartbeat must be a positive integer (nanoseconds)".
- `testCreateMirrorRespectsExplicitMirrorDirectFalse` - an explicit mirror_direct:false survives the mirror default (which fills only an unset value) and is never rewritten to true.
- `testCreateSameBucketSourceWithoutExternalStillGetsTransform` - the ADR-57 transform-skip rule is EXTERNAL-only: a same-domain source of the SAME bucket name still carries the mandatory transform.
- `testCreateExternalSourcesSkipTransformOnlyForSameBucketName` - both halves of the skip rule are pinned on the wire in one create: an EXTERNAL source of the same bucket name carries no transform, while an external source of a DIFFERENT bucket keeps the mandatory one.
- `testCreateExternalSourceNumericBucketAliasComparesAsString` - a numeric `bucket` alias is stringified before the same-bucket comparison, so on a bucket named "42" an external source aliasing bucket 42 counts as the same bucket and carries no transform.
- `testCreateSourceNumericNameIsStringifiedBeforePrefixCheck` - a numeric source `name` is stringified before the KV_ prefix check and resolves exactly like its string form (name KV_42, transform from $KV.42.>).
- `testCreateMirrorNumericNameIsStringifiedBeforePrefixCheck` - a numeric mirror `name` is stringified before the KV_ prefix check too, resolving to mirror name KV_42.
- `testBindResolvesPrefixesFromNumericMirrorName` - bind() resolves the write-through prefix from a mirror name that arrives as a JSON number, publishing to $KV.314.<key> instead of crashing the prefix parse.
- `testCreateMirrorEmptyExternalApiTreatedAsSameDomain` - an EMPTY external api on a mirror means no external at all: writes go through to the origin's own subject with no api prefix glued on, and reads stay on this bucket's prefix.
- `testCreateMirrorExternalApiTrailingDotIsTrimmed` - a trailing dot on the external api is trimmed before the write prefix is assembled, so writes target $JS.HUB.API.$KV.src.<key> and never an empty-token "API..$KV" subject.
- `testBindKvShapeGateBoundaryZeroRejectedOneAccepted` - the KV-shape gate boundary: max_msgs_per_subject exactly 0 is rejected with the full diagnostic, while exactly 1 (the smallest KV-shaped value) binds and returns the same handle.
- `testGetFallbackToleratesNumericDataAndHdrsFields` - the leader STREAM.MSG.GET fallback tolerates loosely typed fields: a numeric 'data' decodes via its string form and a numeric 'hdrs' that is not a NATS/1.0 block yields no headers, so the entry is a plain PUT at the reported revision.
- `testGetAllBatchedNonUnavailableErrorFailsEnumeration` - a batched Direct Get error that is NOT 503 fails getAll() verbatim (code 500 and description), with no per-subject fan-out attempted afterwards to mask it.
- `testGetAllFanOutLeaderFallback404OmitsKey` - when a per-key Direct Get answers 503 and the leader STREAM.MSG.GET fallback reports 404, the key is omitted null-safely (any PHP warning is escalated to a failure), leaving an empty map.
- `testHistoryCompletesWhenRevisionsArriveDuringTheWaitLoop` - history() completes when its revision arrives only DURING the bounded wait loop, because the stall clock measures real elapsed time, so a healthy replay is never failed.
- `testKeysCompleteWhenRecordsArriveDuringTheWaitLoop` - keys() enumerates a headers-only record that lands only during the wait loop, completing within the progress bound and still unsubscribing the deliver subscription.
- `testFailedRecreateKeepsConfirmedMirrorPrefixes` - prefixes the server already CONFIRMED survive a later failed create(): after a rejected re-create the handle still routes writes through the external API to the origin subject and reads the origin's subject, never stranding on the plain bucket subject.

### tests/Unit/MessageTtlTest.php
- `testFormatsIntegerSeconds` - `format(30)` renders the integer as a seconds duration "30s".
- `testFormatsIntegerStringAsSeconds` - `format('45')` treats a bare integer string as seconds, producing "45s".
- `testFormatsDurationStringUnchanged` - `format('1h30m')` passes a Go-style duration string through unchanged.
- `testFormatsNever` - `format('never')` passes "never" through unchanged.
- `testRejectsZeroSeconds` - `format(0)` throws `JetStreamException` (zero/sub-second TTL rejected).
- `testRejectsNegativeSeconds` - `format(-5)` throws `JetStreamException` (negative TTL rejected).
- `testRejectsEmptyString` - `format('   ')` (whitespace-only) throws `JetStreamException`.
- `testRejectsNegativeDurationString` - `format('-5s')` throws `JetStreamException` (negative duration string rejected).
- `testRejectsZeroDurationString` - `format('0s')` throws `JetStreamException` (zero-valued duration string rejected).
- `testNormalizesNeverCaseInsensitively` - `format('NEVER')` is accepted case-insensitively and normalized to "never".
- `testRejectsZeroStringAsSeconds` - `format('0')` throws `JetStreamException` with message "Per-message TTL must be at least 1 second".

### tests/Unit/MonotonicClockUsageTest.php
- `testSrcContainsNoWallClockMicrotime` - Recursively scans every `.php` file under `src/` and asserts none contains the string `microtime(`, enforcing monotonic (hrtime-based) timing in production code.

### tests/Unit/Mutation/AccountInfoMutationTest.php
- `testAllMissingFieldsDefaultToExactlyZero` - Asserts `AccountInfo::fromArray([])` defaults memory, storage, streams and consumers to exactly 0, so no absent-key default can drift to 1 or -1.
- `testStreamsReadsPresentNonZeroValue` - Asserts a present non-zero "streams" value is read from the payload (7) rather than being replaced by the coalesce default.
- `testConsumersReadsPresentNonZeroValue` - Asserts a present non-zero "consumers" value is read from the payload (3) rather than being replaced by the coalesce default.
- `testEachFieldReadsItsOwnPresentValue` - Asserts each of memory, storage, streams and consumers reads its own present payload value, proving the defaults are not always applied and the fields are not cross-wired.

### tests/Unit/Mutation/AmpSocketTransportMutationTest.php
- `testConnectTimeoutFloorClampsToOne` - Asserts the connect timeout floor clamps to exactly 1 ms (not 0 and not 2) for both a 1 ms and a 0 ms dial timeout, read back from the transport's recorded last connect timeout after a successful loopback connect.
- `testHandshakeFirstWithoutTlsContextDoesNotUpgrade` - Asserts handshake-first does not run a TLS handshake when no TLS context was configured, so a plaintext connection with tlsHandshakeFirst set connects cleanly and reports TLS inactive instead of throwing.
- `testTlsRequiredExceptionMessageIsExact` - Asserts the TlsRequiredException raised by an unconfigured TLS upgrade carries its full message assembled from all three operands in order, with none reordered or dropped.
- `testWriteSendsBytesToPeer` - Asserts `write()` actually pushes bytes to the socket, with the peer receiving the exact "PING\r\n" payload.
- `testUppercaseTlsSchemeStillEnablesTls` - Asserts an uppercase "TLS://" DSN scheme is lowercased before the scheme comparison, so a TLS context is still attached to the connect context.
- `testNonEmptyPeerNameOverrideIsKept` - Asserts a non-empty tlsPeerName override is used verbatim as the TLS peer name and never falls back to the DSN host.
- `testEmptyPeerNameFallsBackToDsnHost` - Asserts that with no peer-name override the TLS peer name falls back to the DSN host rather than collapsing to an empty string.
- `testPeerVerificationStaysEnabledByDefault` - Asserts peer verification stays enabled when tlsVerifyPeer is true, so the TLS context is never built without peer verification.
- `testNonEmptyCaFileIsApplied` - Asserts a configured non-empty CA file path is applied to the TLS context.
- `testEmptyCaFileIsNotApplied` - Asserts an empty CA file setting is not applied, leaving the TLS context with no CA file instead of an empty one.
- `testNonEmptyCertFileIsApplied` - Asserts a configured non-empty client certificate file produces a Certificate on the TLS context carrying that cert path.

### tests/Unit/Mutation/AmpSocketTransport_1MutationTest.php
- `testChunkSizeNotAppliedToNonResourceSocket` - Asserts the read-chunk-size guard requires both a ResourceSocket and a positive size, so applying the chunk size to a plain Socket that has no setChunkSize() method is a clean no-op instead of a fatal undefined-method error (#119).

### tests/Unit/Mutation/AmpSocketTransport_2MutationTest.php
- `testCloseClosesUnderlyingSocket` - Asserts `close()` really closes the underlying socket rather than only dropping the transport's reference to it, verified on a retained in-memory socket double.
- `testMalformedDsnHostIsCastToStringForPeerName` - Asserts a malformed, hostless DSN still yields a string TLS peer name (the empty string), because the parsed host is cast to string before it becomes the peer-name fallback.

### tests/Unit/Mutation/BasicJsonSchemaValidatorMutationTest.php
- `testNonObjectTypeDoesNotRunRequiredCheck` - Asserts the required-property check runs only for an object-typed schema over an array value, so a schema of type "array" with a "required" entry validates a JSON list without error.
- `testMalformedPropertySchemaIsSkippedNotBreaking` - Asserts a malformed (non-array) property schema is skipped and iteration continues, so a later valid property's type error is still reported.
- `testAbsentPropertyIsSkippedNotBreaking` - Asserts a declared property absent from the payload is skipped and iteration continues, so a later present property's type error is still reported.
- `testStringArmRejectsNonString` - Asserts the "string" type arm rejects an integer value with "$ must be string, got int" rather than falling through to the permissive default.
- `testBooleanArmRejectsNonBoolean` - Asserts the "boolean" type arm rejects an integer value with "$ must be boolean, got int" rather than falling through to the permissive default.
- `testNullArmRejectsNonNull` - Asserts the "null" type arm rejects an integer value with "$ must be null, got int" rather than falling through to the permissive default.
- `testNumberArmRejectsNonNumber` - Asserts the "number" type arm rejects a string value with "$ must be number, got string", pinning both the arm itself and its is_int-or-is_float test.
- `testNumberArmAcceptsInteger` - Asserts the "number" type arm accepts an integer value, so the is_int side of the check is not negated away.
- `testArrayArmAcceptsJsonList` - Asserts the "array" type arm accepts a JSON list, so neither half of its is_array-and-array_is_list check can be negated without failing.

### tests/Unit/Mutation/BatchPublisherMutationTest.php
- `testOverflowMessageIsExactConcatenation` - Asserts adding past MAX_MESSAGES throws with the exact message "Atomic batch is limited to 1000 messages", its literal, constant and suffix concatenated in that order.
- `testStartRequestUsesFirstStagedMessage` - Asserts the batch START request (Nats-Batch-Sequence:1) carries the first staged message rather than the second, publishing "alpha.subject" with payload AAA and never "beta.subject".
- `testStartDepth512ParsesEmbeddedErrorAndRejects` - Asserts the start-reply parser uses a JSON depth limit of 512, so a rejection document needing exactly that depth still parses and surfaces its embedded error description instead of being swallowed as an accept.
- `testStartDepth513OverflowsAndIsTreatedAsAccepted` - Asserts a start reply nested one level past the depth-512 limit fails to parse, is swallowed and treated as accepted, letting the commit proceed and return its PubAck.
- `testStartRejectionMissingCodeDefaultsToZero` - Asserts a start rejection carrying no "code" field surfaces a JetStreamException with code exactly 0 and the server description preserved.
- `testStartRejectionUsesProvidedCode` - Asserts a start rejection carrying an explicit code uses that code (503) rather than the default.
- `testCommitAckDepth512ParsesEmbeddedError` - Asserts the commit-ack parser uses a JSON depth limit of 512, so an error document needing exactly that depth parses and surfaces its embedded description and code instead of the generic malformed-ack message.
- `testCommitAckDepth513OverflowsToMalformed` - Asserts a commit ack nested one level past the depth-512 limit fails to parse and surfaces the generic "Malformed atomic batch commit ack" message without leaking the embedded description.
- `testMalformedCommitAckExactMessageAndCode` - Asserts a non-JSON commit ack throws with the exact message "Malformed atomic batch commit ack: Syntax error" and code 0.
- `testCommitAckNumericDescriptionIsStringified` - Asserts a commit-ack error whose description is a JSON number is cast to a string before becoming the exception message ("12345"), so no TypeError is raised in its place.
- `testCommitAckErrorMissingCodeDefaultsToZero` - Asserts a commit-ack error carrying no "code" field surfaces a JetStreamException with code exactly 0 and the description preserved.
- `testCommitAckErrorUsesProvidedIntegerCode` - Asserts a commit-ack error carrying an integer code uses that code (409) rather than the default.
- `testCommitAckStringCodeIsCastToInt` - Asserts a commit-ack error whose "code" is a numeric string is cast to int before becoming the exception code (409), so no TypeError is raised in its place.

### tests/Unit/Mutation/BatchPublisher_1MutationTest.php
- `testPreflightRejectsSubTwoMajorVersion` - Asserts a sub-2.0 MAJOR INFO version ("1.99") trips the commit() pre-flight, throwing UnsupportedFeatureException naming that server version with zero batch frames written, so the major component (not the minor) drives the comparison even though replies for the would-be proceed path are scripted (#152).
- `testUnanchoredVersionDoesNotTripPreflight` - Asserts a version string that does not begin with an (optionally v-prefixed) number ("release-1.5") counts as unparseable so the anchored regex skips the pre-flight and the 2-message batch commits normally, rather than reading the embedded "1.5" as major 1 and rejecting (#152).
- `testVPrefixedVersionUsesCaptureGroupNotFullMatch` - Asserts a v-prefixed "v2.12.0" takes its major from the regex capture group ("2") and not the full match ("v2.12", which casts to 0), so a fully batch-capable server passes the pre-flight and the batch commits (#152).
- `testStartRejectedWhenReplyCarriesStreamButNoSeq` - Asserts a batch START acknowledged with a normal-PubAck shape carrying only "stream" and no "seq" aborts the commit with UnsupportedFeatureException (feature allow_atomic), so either field alone is enough to detect that the server stored the start as a plain publish (#130).
- `testPreflightMessageSaysNothingPublishedAndNamesVersion` - Asserts the pre-flight failure message reads exactly "Atomic batch publish requires NATS server 2.12+ (connected server 2.11.4; nothing was published)" and that nothing reaches the wire, pinning both the version token and the "data is intact" wording (#152).
- `testReplyShapeMessageSaysTreatedAsPlainPublishes` - Asserts the reply-shape abort on an unparseable-version server whose start is acked as a plain publish reads exactly "Atomic batch publish requires NATS server 2.12+ (connected server synadia-custom treated the batch as plain publishes)", so the orphan-start case is never mislabelled as "nothing was published" (#152).

### tests/Unit/Mutation/ConsumerInfoMutationTest.php
- `testNonStringFalsyDeliverSubjectIsCastBeforeEmptinessCheck` - Asserts a non-string falsy `deliver_subject` (bool false) is cast to '' before the emptiness check, so fromArray() reports a pull consumer (push=false) instead of letting the strict comparison against '' mark it push-based.
- `testPresentDeliverSubjectMakesConsumerPush` - Asserts a real, non-empty `deliver_subject` marks the hydrated consumer push-based (push=true), pinning the other side of the cast and emptiness boundary.
- `testNameFallsBackToDurableNameWhenTopLevelNameMissing` - Asserts the consumer name falls back to the nested `config.durable_name` when the top-level `name` is absent, rather than collapsing to an empty string.
- `testTopLevelNameTakesPrecedenceOverDurableName` - Asserts the top-level `name` wins over a differing `config.durable_name` when both are present, pinning the coalesce precedence.

### tests/Unit/Mutation/CredentialsParserMutationTest.php
- `testFromFileRejectsDirectoryPathBecauseGuardIsOr` - Asserts a directory path (not a file, yet readable) trips fromFile()'s readability guard with a NatsException saying "not found or not readable", rather than falling through to read the directory and failing later with a different parse error.
- `testNotReadableMessageOrderingAndIncludesPath` - Asserts the not-found/not-readable message is exactly the literal prefix "Credentials file not found or not readable: " followed by the offending path, with the operands in that order and the path never dropped.
- `testExtractedBlocksAreTrimmed` - Asserts the JWT and NKEY seed blocks are trimmed, so whitespace captured inside each block ("  jwt-value-x  ", "\tseed-value-y\t") is stripped from the parsed credentials.

### tests/Unit/Mutation/FeatureSupportMutationTest.php
- `testMatchesUnknownFieldPhraseCaseInsensitively` - Asserts the "unknown field" phrase in a server API error is matched case-insensitively, so both an all-caps report naming allow_atomic and a mixed-case one naming allow_msg_ttl map to a typed UnsupportedFeatureException carrying the captured feature and its required version (2.12 and 2.11) instead of returning null.

### tests/Unit/Mutation/InboxMutationTest.php
- `testRandomSuffixIsExactlyTwentyFourHexChars` - Asserts Inbox::generate() appends exactly 24 lowercase hex characters after the prefix dot (bin2hex of 12 random bytes), pinning the entropy width against a byte count one lower or higher.
- `testTotalLengthIsPrefixPlusDotPlusTwentyFourHex` - Asserts the generated subject's total length is the prefix plus one dot plus 24 hex characters for a multi-token prefix, re-pinning the suffix width through the full string.

### tests/Unit/Mutation/JetStreamContext_10MutationTest.php
- `testSecondPullReplyTokenIncrementsRatherThanDecrements` - Asserts the pipelined pull engine mints each pull's reply-subject token from a monotonically increasing counter, so two serial pulls carry the tokens "0" then "1" on the wire (never a 64-bit wraparound like "ffffffffffffffff") while both messages are still delivered in order (#120).

### tests/Unit/Mutation/JetStreamContext_11MutationTest.php
- `testGroupedRunStaysSerialUntilPinResolved` - Asserts a grouped pull run whose pin id is not yet resolved stays serial (effective depth 1), so at depth 2 only the first pull is on the wire when its first message is delivered and a pinned fan-out cannot race the server's pin assignment (#120).
- `testIdleStreakClampsEffectiveDepthToOne` - Asserts an active empty-pull streak clamps the effective depth to 1, so the generation following a fully empty depth-2 generation issues exactly one pull (three pull requests in total) instead of fanning out again, keeping the idle poll rate at one pull per backoff window (#153).
- `testZeroDepthIsFlooredToOneAndStillDelivers` - Asserts consumePipelined() floors a depth-0 config to one live pull, so the run still issues a pull request and delivers its message rather than returning zero with nothing in flight.

### tests/Unit/Mutation/JetStreamContext_12MutationTest.php
- `testStatusForRetiredPullTokenIsDroppedNotReprocessed` - Asserts a status frame that arrives late on an already-retired pull token is dropped instead of being applied to a pull that no longer exists, so the run still reports one processed message and one surfaced error rather than failing on a missing pull (#120).
- `testReconnectReissuesFreshPullInInfiniteMode` - Asserts a reconnect in infinite mode clears the in-flight pulls and issues a fresh pull, so the message delivered after the second connect reaches the handler instead of the run staying stuck on the retired pre-reconnect pull.
- `testReconnectResetsConsecutiveEmptyPullsToZero` - Asserts the reconnect reset seeds the consecutive-empty-pull counter at 0, so the post-reconnect empty streak clamps the next generation to a single pull and the run writes exactly six pull requests.
- `testReconnectClearsBackoffWarranted` - Asserts the reconnect reset clears the backoff-warranted flag, so a post-reconnect waiting-empty streak does not arm the backoff and the next generation still fans out to depth 2 for seven pull requests in total.
- `testStalePin423RetireDrainsNextReadyPullInSamePass` - Asserts a 423 stale-pin retire keeps draining the ready generation in the same pass, so the next ready pull's message is delivered after exactly two pull requests with no fresh pull issued in between.
- `testInflightNonEmptyIdleDrainingDoesNotUnlatchAndOverIssue` - Asserts the generation-boundary backoff block is entered only when nothing is still in flight, so an idle-draining latch held while a refilled pull is outstanding stays latched and no extra pull is issued (three pull requests in total) (#169).
- `testBackoffWarrantedResetSoWaitingEmptiesNeverArmBackoffAcrossGenerations` - Asserts plain waiting empties never arm the backoff across generations, so the empty streak stays cleared at every boundary and each generation still fans out to depth 2 instead of the later generation being clamped to a single pull (#169).

### tests/Unit/Mutation/JetStreamContext_13MutationTest.php
- `testDeleteStreamCastsNonBooleanSuccessToBool` - Asserts deleteStream() casts a non-boolean success flag (the integer 1) to a real boolean true rather than returning the raw response value.
- `testDeleteMessageCastsNonBooleanSuccessToBool` - Asserts deleteMessage() casts a non-boolean success flag on the STREAM.MSG.DELETE ack to a real boolean true.
- `testDeleteConsumerCastsNonBooleanSuccessToBool` - Asserts deleteConsumer() casts a non-boolean success flag on the CONSUMER.DELETE ack to a real boolean true.
- `testListStreamsDropsNonArrayStreamEntry` - Asserts listStreams() filters non-array entries out of a page before hydrating StreamInfo, returning only the one well-formed stream.
- `testListConsumersDropsNonArrayConsumerEntry` - Asserts listConsumers() filters non-array entries out of a CONSUMER.LIST page before hydrating ConsumerInfo, returning only the one well-formed consumer.
- `testSubscribeOrderedConsumerRejectsZeroIdleHeartbeat` - Asserts subscribeOrderedConsumer() rejects an idle heartbeat of exactly 0 with an InvalidArgumentException, pinning zero as the rejected boundary of the heartbeat guard.
- `testDeleteConsumerRejectsInvalidStreamName` - Asserts deleteConsumer() validates the stream name itself, rejecting a dotted name with "Invalid stream name" instead of letting the request reach the wire.
- `testPauseConsumerRejectsInvalidStreamName` - Asserts pauseConsumer() rejects a dotted stream name with "Invalid stream name" before issuing its request.
- `testResumeConsumerRejectsInvalidStreamName` - Asserts resumeConsumer() rejects a dotted stream name with "Invalid stream name" before issuing its request.
- `testUnpinConsumerRejectsInvalidStreamName` - Asserts unpinConsumer() rejects a dotted stream name with "Invalid stream name" even when the group name is valid.
- `testDirectGetLastForSubjectsRejectsInvalidStreamName` - Asserts directGetLastForSubjects() validates the stream name even for an empty subject list, which returns before delegating to directGetBatch, so an invalid stream still rejects instead of silently returning an empty result.

### tests/Unit/Mutation/JetStreamContext_1MutationTest.php
- `testDefaultRetryAttemptsAllowsTwoRetriesBeforeSuccess` - Asserts the default publishRetryAttempts of 3 lets two transient 503s be retried so the third attempt's ack still resolves the publish.
- `testDefaultRetryAttemptsStopsAfterThreeAttempts` - Asserts the default publishRetryAttempts of 3 is exhausted by a third consecutive 503, which throws JetStreamException with code 503 instead of making a fourth attempt.
- `testBatchIdOf64CharsIsAccepted` - Asserts a 64-character batch id sits on the inclusive upper bound and is accepted, with the publisher reporting the id verbatim.
- `testBatchIdOf65CharsIsRejected` - Asserts a 65-character batch id is rejected with "Batch id must be between 1 and 64 characters", pinning the far side of that bound.
- `testGeneratedBatchIdIs32HexChars` - Asserts a generated batch id is exactly 32 lowercase hex characters.
- `testInvalidBucketMessageHasExactConcatenation` - Asserts the invalid-bucket error reads exactly `Invalid bucket name "<bucket>": only letters, digits, "-" and "_" are allowed`, pinning both operand order and every concatenated part.
- `testCreateStreamPayloadCarriesName` - Asserts createStream() merges the stream name into the request body alongside the subjects.
- `testUpdateStreamPayloadCarriesName` - Asserts updateStream() sends its request to STREAM.UPDATE with both the stream name and the caller-supplied config field in the body.
- `testCreateOrUpdateFallbackMergesOptionsAndSubjects` - Asserts the create-then-update fallback sends an UPDATE body carrying both the original options and the subjects, so neither operand of the merge is lost.
- `testStreamNamesFirstRequestUsesOffsetZero` - Asserts the first STREAM.NAMES request carries an explicit "offset":0 rather than omitting the key or starting negative.
- `testStreamNamesIncludesSubjectFilterWhenProvided` - Asserts a non-empty subject filter is sent as the "subject" field of the STREAM.NAMES body.
- `testStreamNamesOmitsSubjectFilterWhenNull` - Asserts streamNames() with no subject filter sends a body carrying no "subject" key at all.
- `testStreamNamesOmitsSubjectFilterWhenEmptyString` - Asserts an empty-string subject filter is treated as no filter, so the body still carries no "subject" key.
- `testStreamNamesPaginatesAcrossTwoPages` - Asserts streamNames() keeps requesting pages while the running count is below the reported total, concatenating both pages into one ordered list.
- `testStreamNamesAdvancesOffsetCumulatively` - Asserts streamNames() advances the page offset cumulatively across three pages (0, 2, 4) instead of resetting or decreasing it, returning all five names in order.
- `testStreamNamesFiltersNonStringEntries` - Asserts non-string entries in a "streams" page are filtered out so only the string names are returned.
- `testDeleteStreamDefaultsToFalseWhenSuccessAbsent` - Asserts deleteStream() returns false when the response omits "success", so a silent reply is never reported as a successful delete.
- `testPurgeStreamDefaultsToZeroWhenPurgedAbsent` - Asserts purgeStream() reports 0 purged when the response omits "purged".
- `testPurgeStreamCastsPurgedToInt` - Asserts a non-int "purged" value is cast to an integer in the returned array rather than left as the raw string.
- `testListStreamsAdvancesOffsetCumulatively` - Asserts listStreams() advances the page offset cumulatively across three pages (0, 2, 4) and returns all five hydrated streams in order.
- `testListConsumersFirstRequestUsesOffsetZero` - Asserts the first CONSUMER.LIST request targets the stream's CONSUMER.LIST subject with an explicit "offset":0.

### tests/Unit/Mutation/JetStreamContext_2MutationTest.php
- `testListConsumersPaginatesAccumulatingOffset` - Asserts listConsumers() keeps requesting pages until the reported total is reached, advancing the offset cumulatively (0, 2, 4) and never negatively, and returns all five consumers in order.
- `testStreamMessageIgnoresNonStringData` - Asserts a stream message whose "data" field is not a string is left with an empty payload rather than being base64-decoded from a coerced value, while its subject is still parsed.
- `testStreamMessageSidIsZero` - Asserts a message built from a STREAM.MSG.GET response carries the synthetic sid 0, so it is never tagged as a routed subscription delivery.
- `testDeleteMessageReturnsFalseWhenSuccessAbsent` - Asserts deleteMessage() returns false when the response omits the success flag.
- `testDirectGetNoRespondersMessageIsExact` - Asserts a Direct Get against a stream with no DIRECT.GET responder throws code 503 with the exact message naming the stream and the allow_direct guidance, pinning every operand of that text.
- `testDirectGetThrowsAtStatus400Boundary` - Asserts a Direct Get status of exactly 400 still throws, surfacing the status description with code 400 rather than falling through to the unrecognized-response guard.
- `testDirectGetAcceptsResponseWithOnlyNatsStreamAndSidIsZero` - Asserts a Direct Get reply carrying Nats-Stream but no Nats-Sequence is accepted (only a response missing both metadata headers is rejected) and the returned message carries sid 0.
- `testDirectGetLastForSubjectsWildcardMessageIsExact` - Asserts directGetLastForSubjects() rejects a wildcard subject with the exact message quoting the subject and pointing at directGetBatch(), pinning operand order and every literal.
- `testDirectGetBatchThrowsAtStatus400WithDescription` - Asserts directGetBatch() treats a status of exactly 400 as an error and throws with code 400 and the server-provided description instead of the generic fallback text.
- `testDirectGetBatchMessageSidIsZero` - Asserts directGetBatch() builds each collected message with sid 0 while preserving its payload.

### tests/Unit/Mutation/JetStreamContext_3MutationTest.php
- `testDirectGetBatchCollectedMessageHasSidZero` - Asserts directGetBatch() builds every collected message with sid 0, so a batch reply is never tagged with a subscription id that would make it look like a routed delivery.
- `testDirectGetBatchUnsubscribesAfterCompletion` - Asserts directGetBatch() unsubscribes from its private inbox once the batch terminates at EOB, writing an UNSUB rather than leaking the subscription for the rest of the connection.
- `testDirectGetBatchSurfacesServerErrorDescription` - Asserts an error status frame's server-provided description ("detailed-server-error") becomes the JetStreamException message, with code 500, instead of being replaced by the generic fallback text.
- `testCreateConsumerSendsStreamName` - Asserts createConsumer() sends stream_name alongside config in the CONSUMER.CREATE body, the field the server requires to bind the consumer to its stream.
- `testAddConsumerSendsStreamName` - Asserts addConsumer() with a typed ConsumerConfiguration sends stream_name alongside config in the CONSUMER.CREATE body.
- `testCreateEphemeralConsumerSendsStreamName` - Asserts createEphemeralConsumer() sends stream_name in the CONSUMER.CREATE body.
- `testCreatePushConsumerSendsStreamName` - Asserts createPushConsumer() sends stream_name in the CONSUMER.CREATE body.
- `testCreateEphemeralPushConsumerSendsStreamName` - Asserts createEphemeralPushConsumer() sends stream_name in the CONSUMER.CREATE body.
- `testCreateEphemeralConsumerValidatesPriorityConfig` - Asserts createEphemeralConsumer() still validates priority config before dispatch, rejecting an unknown priority_policy with JetStreamException ("priority_policy must be one of") while no CREATE request reaches the wire.
- `testConsumerNamesSendsZeroOffsetOnFirstRequest` - Asserts consumerNames() sends an explicit "offset":0 on the first page request, so pagination starts at the head of the list rather than omitting the key or starting negative.
- `testConsumerNamesPaginatesAndAccumulatesOffset` - Asserts consumerNames() keeps requesting pages until the reported total is reached, advancing the offset by each page's size (0, 2, 4) rather than stopping after one page or re-requesting the same offset, and returns all six names in order.
- `testConsumerNamesFiltersNonStringsAndReindexes` - Asserts consumerNames() drops non-string entries from a page and returns a cleanly reindexed list of names.
- `testSubscribePushConsumerUsesProvidedDeliverSubject` - Asserts subscribePushConsumer() honours a caller-supplied deliver subject for both the deliver_subject in the CREATE body and the SUB it registers, never generating an _INBOX.JS.PUSH inbox in its place.
- `testSubscribeOrderedConsumerSetsMaxDeliverOne` - Asserts the ordered ephemeral consumer is created with max_deliver exactly 1, the setting that makes redelivery impossible so gap detection alone drives recovery.
- `testOrderedConsumerRecreatesFromSequenceOneWhenGapOnFirstDelivery` - Asserts a gap on the very first delivery (before any in-order message advances the last stream sequence) triggers exactly one recreate that re-applies the consumer's initial deliver policy, emitting neither by_start_sequence nor opt_start_seq, so a 'new' or 'last_per_subject' watch is not silently rewound to a full stream replay.
- `testOrderedConsumerResetsExpectedSequenceToOneAfterRecreate` - Asserts the expected consumer sequence resets to 1 after a recreate, so the recreated consumer's first delivery (consumer seq 1) is accepted and handed to the handler instead of being discarded as another gap.
- `testOrderedConsumerTailGapRecreatesWhenHeartbeatExactlyOneAhead` - Asserts an idle heartbeat reporting a last-consumer sequence exactly one ahead of what was processed is treated as a tail gap and recreates exactly once (one DELETE, a second CREATE), pinning the lower edge of the tail-gap threshold.
- `testOrderedConsumerTailGapNoRecreateWhenHeartbeatCaughtUp` - Asserts an idle heartbeat reporting a last-consumer sequence equal to the highest processed sequence is caught up and triggers no recreate at all (no DELETE, still one CREATE), with the delivered message retained.
- `testDeleteConsumerDefaultsToFalseWhenSuccessAbsent` - Asserts deleteConsumer() returns false when the server response omits "success", so a silent response is never reported as a successful delete.
- `testUnpinConsumerReturnsFalseOnExplicitFailure` - Asserts unpinConsumer() honours an explicit success:false in the response instead of always reporting success.
- `testNoResponderMessageHasPrefixThenSubject` - Asserts a JetStream publish to a subject with no responder surfaces a 503 JetStreamException whose message leads with "No JetStream responder for subject" and embeds the target subject.

### tests/Unit/Mutation/JetStreamContext_4MutationTest.php
- `testNoResponderMessageKeepsSubjectFirstAndSuffix` - Asserts the no-responders error leads with "No JetStream responder for subject" plus the target subject and keeps its trailing parenthetical about the subject not being bound to a stream.
- `testSingleAttemptDoesNotRetryOn503` - Asserts publishRetryAttempts=1 means no retry at all, so a single 503 surfaces immediately as JetStreamException with code 503 rather than a second attempt consuming the queued ack.
- `testTwoAttemptsRetryOnceThenSucceed` - Asserts publishRetryAttempts=2 retries the first 503 once and returns the ack from the retry.
- `testScheduledPublishDefaultsRollupOff` - Asserts publishScheduled() defaults rollup to off, emitting no Nats-Schedule-Rollup header when the caller does not ask for one.
- `testScheduledPublishOmitsEmptySource` - Asserts publishScheduled() given an empty source string emits no Nats-Schedule-Source header.
- `testMalformedPublishAckMessageAndCode` - Asserts a non-JSON publish ack is wrapped as a JetStreamException whose message starts with "Malformed JetStream publish ack: ", appends the JSON parser detail, and carries code 0.
- `testPublishApiErrorWithoutCodeDefaultsToZero` - Asserts a publish-ack API error with no code field maps to JetStreamException code 0 while preserving the server description.
- `testPublishApiErrorPropagatesProvidedCode` - Asserts a publish-ack API error carrying a code propagates that code instead of collapsing to 0.
- `testCounterDeltaIsTrimmedBeforeValidation` - Asserts incrementCounter() trims the delta before validating it, so a whitespace-padded integer is accepted and the trimmed value is sent as the Nats-Incr header.
- `testCounterDeltaRejectsLeadingGarbage` - Asserts the counter delta regex is anchored at the start, so a delta with leading garbage is rejected with "Counter increment must be an integer string" and no request reaches the wire.
- `testMalformedCounterResponseMessageAndCode` - Asserts a non-JSON counter response is wrapped as a JetStreamException whose message starts with "Malformed counter response: ", appends the JSON parser detail, and carries code 0.
- `testCounterApiErrorWithoutCodeDefaultsToZero` - Asserts a counter API error with no code field maps to JetStreamException code 0 while preserving the server description.
- `testCounterApiErrorPropagatesProvidedCode` - Asserts a counter API error carrying a code propagates that code instead of collapsing to 0.
- `testFetchNextDefaultExpiryAndBatchOfOne` - Asserts fetchNext() defaults its expiry to 3000 ms (sent as "expires":3000000000) and always pulls a batch of exactly 1.
- `testFetchBatchDefaultExpiry` - Asserts fetchBatch() defaults its expiry to 3000 ms (sent as "expires":3000000000) when none is supplied.

### tests/Unit/Mutation/JetStreamContext_5MutationTest.php
- `testFetchBatchTreats400AsTerminal` - Asserts fetchBatch() treats a status of exactly 400 as terminal, throwing a JetStreamException with code 400 instead of collecting the status frame as a message.
- `testFetchBatchUnsubscribesAfterCompletion` - Asserts fetchBatch() unsubscribes from its fetch inbox once the pull completes, writing the UNSUB from the finally block rather than leaking the subscription.
- `testScheduleIsTrimmedBeforeValidation` - Asserts publishScheduled() trims the schedule string before validating it, so "  @daily  " passes the alias check and is published as Nats-Schedule:@daily.
- `testScheduleAtRegexIsAnchored` - Asserts the @at schedule regex is anchored at both ends, so a timestamp with leading or trailing junk is rejected with "Unsupported schedule expression" before any request reaches the wire.
- `testScheduleEveryRegexIsAnchored` - Asserts the @every schedule regex is anchored at the start, so "pre@every 1h" is rejected instead of matching mid-string.
- `testSchedulePredefinedAliasRegexIsAnchored` - Asserts the predefined-alias regex is anchored at both ends, rejecting "x@daily" and "@dailyz" before dispatch.
- `testUnsupportedScheduleExceptionMessageExact` - Asserts the unsupported-schedule exception message is exactly the fixed template with the offending value quoted in place, pinning both the concatenation order and every operand.
- `testIsCronScheduleTrimsBeforeClassifying` - Asserts the cron classifier trims first, so a leading-space "@every 1h" still classifies as non-cron and a time zone supplied with it is rejected with "only valid for cron".
- `testIsCronScheduleRegexIsAnchored` - Asserts the cron classifier's @at/@every regex is anchored at the start, so the 6-field cron "a @at b c d e" stays cron-class and its time zone is accepted and sent as Nats-Schedule-Time-Zone.
- `testHeartbeatLastConsumerRequiresAllDigits` - Asserts an ordered consumer only treats a heartbeat's Nats-Last-Consumer as a tail gap when it is both non-empty and all digits, so a value of "9x" leaves exactly one CREATE on the wire and no DELETE.
- `testFilterSubjectsAreReindexedToArray` - Asserts filter_subjects is reindexed with array_values so a gap-indexed input still encodes as a JSON array rather than an object.
- `testApplyFilterSubjectsReturnsFullConfig` - Asserts applying filter_subjects returns the complete consumer config, so durable_name and ack_policy survive alongside the filter list in the CREATE body.
- `testPullGroupRegexIsAnchored` - Asserts the pull-group regex is anchored at the start, so a group with an invalid leading character is rejected with "Pull group must be" before any request is dispatched.
- `testPullPriorityZeroIsAccepted` - Asserts pull priority 0 is the inclusive lower bound and is dispatched as "priority":0 rather than rejected.
- `testPullPriorityNineIsAccepted` - Asserts pull priority 9 is the inclusive upper bound and is dispatched as "priority":9 rather than rejected.
- `testPullPriorityRejectsNonInteger` - Asserts a non-integer pull priority such as the string "5" is rejected with "Pull priority must be an integer" before dispatch.
- `testPriorityGroupsRegexIsAnchored` - Asserts the priority_groups name regex is anchored at the start, so "!abc" is rejected with "priority_groups names must be" before any CREATE request is written.
- `testEmptyRequestBodyEncodesAsObject` - Asserts an empty JetStream API request body is encoded as the JSON object {} and never as the array [].

### tests/Unit/Mutation/JetStreamContext_6MutationTest.php
- `testDecodesJsonAtOneBelowTheDepthLimit` - Asserts an API reply nested 511 levels deep still parses under the 512 decode-depth limit, so deleteStream() returns false without throwing.
- `testRejectsJsonAtTheDepthLimit` - Asserts an API reply nested 512 levels deep is one level too deep and surfaces as a JetStreamException reading "Malformed JetStream API response".
- `testMalformedResponseWrappingMessageAndCode` - Asserts a non-JSON API reply is wrapped as a JetStreamException whose message starts with "Malformed JetStream API response: " and carries the JSON error detail appended after it, with code exactly 0.
- `testApiErrorWithoutCodeDefaultsToZero` - Asserts an API error payload carrying a description but no code surfaces with that description as the message and a code that defaults to exactly 0.
- `testRejectsJsReplyWhenSecondTokenIsNotAck` - Asserts streamSequenceOf() returns null for a "$JS.<not-ACK>..." reply subject, since the guard rejects when either of the two leading tokens is wrong.
- `testReturnsNullWhenFirstTokenIsNotJs` - Asserts streamSequenceOf() short-circuits with null when the reply subject's first token is not "$JS", even though the subject otherwise has a valid 9-token ACK shape.

### tests/Unit/Mutation/JetStreamContext_7MutationTest.php
- `testDirectGetBatchUnsubscribesInboxEvenWhenBatchStalls` - Asserts directGetBatch() UNSUBs its private inbox from the finally block even when a silent server stalls the collect loop into a JetStreamException (#119).
- `testFetchBatchUnsubscribesInboxEvenWhenHeartbeatMissThrows` - Asserts fetchBatch() UNSUBs its private inbox from the finally block even when the pull aborts with a missed-idle-heartbeat JetStreamException (#119).
- `testDirectGetBatchDrainsChunkedMessageWithoutPerChunkIdleSleep` - Asserts directGetBatch() sleeps only on a genuinely idle read, so a message split into more than a thousand one-byte transport chunks is drained and returned well inside the deadline instead of stalling (#119).
- `testFetchBatchDrainsChunkedMessageWithoutPerChunkIdleSleep` - Asserts fetchBatch() likewise loops immediately on byte-consuming reads, so a heavily chunked message is returned rather than blowing the deadline into the 408 "no messages" error (#119).

### tests/Unit/Mutation/JetStreamContext_8MutationTest.php
- `testConsumePipelinedRejectsInvalidStreamNameSynchronously` - Asserts consumePipelined() validates the stream name itself and throws a JetStreamException synchronously, before any Future is handed back (#120).
- `testConsumePipelinedRejectsInvalidConsumerNameSynchronously` - Asserts consumePipelined() also validates the consumer name synchronously, throwing for "bad*consumer" even when the stream name is valid (#120).
- `testFirstPullReplyToIsBaseDotZeroToken` - Asserts the first pull request's reply-to is exactly the pull inbox base joined by a dot to the token "0", pinning both the separator and the token sequence start (#120).
- `testTrailingFrameAfterBatchCompletionIsDropped` - Asserts a frame arriving after its pull's batch is already complete is dropped by the router, so an over-delivered batch of 1 hands the handler only the first message (#120).
- `testIdleHeartbeatIsNotBufferedAsAMessage` - Asserts a status-100 idle heartbeat is never buffered as data, so a batch-1 pull still delivers the real message that follows it (#120).
- `testStatus400IsTerminalAtTheBoundary` - Asserts a status of exactly 400 is terminal, firing onError with a JetStreamException of code 400 and delivering no message (#120).
- `testPullInboxIsExemptFromSlowConsumerBound` - Asserts the long-lived pull inbox is marked unbounded, so three messages arriving in one chunk under a 2-message per-subscription limit still deliver the oldest instead of losing it to the slow-consumer drop (#120).
- `testWaitingEmptyGenerationDoesNotArmBackoff` - Asserts a generation of WAITING empties (404, not no_wait) leaves the backoff unarmed, so the next generation still fans out to the full configured depth (#120, #169).
- `testInfiniteModeDropsInflightPullsOnReconnect` - Asserts an infinite-mode run clears its in-flight pulls on reconnect, so a reply arriving on a pre-reconnect token is dropped rather than delivered (#120).

### tests/Unit/Mutation/JetStreamContext_9MutationTest.php
- `testRoutine409EmptyWarrantsBackoffSoNextGenerationIsSerial` - Asserts a routine (non-terminal) 409 empty streak warrants an idle backoff, so the next generation is issued serially at depth 1 and exactly 4 pull requests reach the wire (#120, #169).
- `testWaiting404EmptyDoesNotWarrantBackoffSoNextGenerationStaysParallel` - Asserts a waiting 404 empty streak does not warrant a backoff, so the next generation stays at depth 2 and exactly 5 pull requests reach the wire (#120, #169).
- `testTerminalErrorAbandonsAlreadyBufferedTailOfGeneration` - Asserts a terminal error breaks out of the retire pass, abandoning a message already buffered on the generation's tail pull instead of going on to deliver it (#120).
- `testDeliveryDrainsAllReadyPullsBeforeIssuingMore` - Asserts a delivery keeps draining every ready pull before any refill pull is issued, so only the generation's two pull requests are on the wire when the second buffered message is handled (#120).
- `testStalePin423DropsPinBeforeRepulling` - Asserts a 423 stale-pin retire drops the captured pin, so the pull issued after it no longer carries the dead "id" (#120).
- `testDeliveryResetsIdleStreakToZeroNotNegative` - Asserts a delivery resets the consecutive-empty counter to exactly 0, so a single warranted empty streak afterwards is enough to force the following generation serial (#120, #169).
- `testDeliveryClearsBackoffWarrantedLatch` - Asserts a delivery clears the backoff-warranted latch, so a later waiting-404 streak does not spuriously back off and the next generation stays at depth 2 (#120, #169).

### tests/Unit/Mutation/JsMessageMetadataMutationTest.php
- `testTimestampMicrosecondsUseDivisorOf1000` - Asserts the sub-second remainder is divided by exactly 1000 to convert nanoseconds to microseconds, so a 999000 ns remainder formats as microsecond field "000999".
- `testTimestampFallbackUsesAtPrefixedUnixSeconds` - Asserts the defensive timestamp fallback builds its date from "@" followed by the unix seconds, so a PHP_INT_MIN nanosecond value that defeats createFromFormat() still yields epoch second -9223372036 (1677-09-21 00:12:44).

### tests/Unit/Mutation/KeyValueBucket_1MutationTest.php
- `testCreateEmitsAllDefaultStreamConfigFields` - Asserts create() writes every KV default into the stream config (description "KV bucket cfg", max_msgs_per_subject:1, allow_direct:true, allow_rollup_hdrs:true, subjects ["$KV.cfg.>"]) and that a non-overlapping user option (max_bytes) is merged in alongside them rather than replacing the defaults.
- `testCreateSourcesAreReindexedToJsonArray` - Asserts create() reindexes mapped sources so they serialize as a JSON array even when supplied under string keys, each entry carrying its mandatory ADR-57 subject transform from the source keyspace to the bucket keyspace.
- `testUpdateValidatesKeyBeforeRevision` - Asserts update() validates the key before the expected revision, so an invalid key combined with a non-positive revision reports "Invalid KV key" rather than the revision error.
- `testDeleteValidatesKey` - Asserts delete() rejects an invalid key with JetStreamException "Invalid KV key" instead of publishing to a malformed subject.
- `testPurgeValidatesKey` - Asserts purge() rejects an invalid key with JetStreamException "Invalid KV key" before any wire activity.
- `testGetValidatesKey` - Asserts get() rejects an invalid key with "Invalid KV key" before issuing any Direct Get request, even when a valid Direct Get reply is queued and would otherwise let the call complete.
- `testGetRevisionValidatesKeyBeforeRevision` - Asserts getRevision() validates the key before the revision, so an invalid key with a non-positive revision surfaces "Invalid KV key" rather than the revision error.
- `testOperationFromHeadersUppercasesKvOperation` - Asserts a lowercase KV-Operation header is upper-cased before matching, so "del" resolves to a DEL tombstone with a null value instead of leaking a live empty value.
- `testStreamMessageFallbackRequestsLastBySubject` - Asserts the STREAM.MSG.GET fallback (after a 503 Direct Get) requests the latest record with a "last_by_subj" key naming the key's KV subject, issuing exactly one such request and returning the value.
- `testStreamMessageFallbackErrorWithoutCodeUsesZero` - Asserts a fallback error envelope carrying no "code" surfaces as a JetStreamException whose code defaults to 0, with the server description preserved.
- `testStreamMessageFallbackCastsStringCodeBeforeComparing` - Asserts a fallback error whose "code" is the string "404" is cast to int before the strict not-found comparison, so the key reads as missing (null) rather than throwing.
- `testStreamMessageFallbackCastsSeqToInt` - Asserts a fallback message's string "seq" is cast to int so the returned entry's revision is a real integer.
- `testWatchFilterSubjectIsPrefixThenPattern` - Asserts the watch consumer's filter_subject is the bucket prefix followed by the requested key pattern, in that order.
- `testWatchIgnoreDeletesSuppressesDeleteDelivery` - Asserts a DEL delivery never reaches the handler when ignoreDeletes is set.
- `testWatchFiresCaughtUpFromDeliveryWhenPendingZero` - Asserts onCaughtUp fires exactly once from the in-handler check when a delivery's $JS.ACK metadata reports pending 0, and does not fire at creation time while the consumer still reports num_pending 1.
- `testWatchFiresCaughtUpImmediatelyWhenNumPendingFieldAbsent` - Asserts an absent num_pending on the created consumer defaults to 0 so onCaughtUp fires immediately, before and without any delivery.

### tests/Unit/Mutation/KeyValueBucket_2MutationTest.php
- `testCreateKeyValidatesKeyBeforePublishing` - Asserts createKey() validates the key before publishing anything, so "a b" throws JetStreamException "Invalid KV key" rather than reaching the wire.
- `testCreateKeyThrowsExactKeyExistsMessageAndCode` - Asserts the collision exception raised when a rejected exclusive create is followed by a live value reads exactly "Key already exists: theme" and carries code 400 with err_code 10071 (#154).
- `testCreateKeyRecreatesAgainstTombstoneRevision` - Asserts createKey() over a DEL tombstone at revision 4 republishes with Nats-Expected-Last-Subject-Sequence:4 and returns the resulting ack.
- `testCreateKeyRecreatesWithZeroSeqWhenTombstoneHasNoRevision` - Asserts a DEL tombstone carrying no Nats-Sequence header resolves to revision null and makes the recreate assert expected sequence 0.
- `testCreateKeyRecreatesWithZeroSeqWhenEntryAbsent` - Asserts a get() that races to a 404 after the rejected create leaves no entry, so the recreate asserts expected sequence 0.
- `testHistoryValidatesKeyBeforeAnyRequest` - Asserts history() rejects an invalid key with JetStreamException "Invalid KV key" before issuing any request.
- `testHistoryRequestsDeliverPolicyAll` - Asserts the history consumer is created on the bucket stream with "deliver_policy":"all" so the full revision replay is requested.
- `testHistoryReturnsEmptyWhenPendingZeroEvenIfDeliveryQueued` - Asserts history() returns [] immediately when the consumer reports num_pending 0, never subscribing to the deliver subject even though a delivery is waiting there.
- `testHistoryTreatsMissingPendingAsZero` - Asserts an absent num_pending on the created consumer defaults to 0, so history() short-circuits to [] without collecting the queued delivery.
- `testHistoryCastsPendingToInt` - Asserts a JSON float 0.0 num_pending is cast to int before the strict zero test, so history() still short-circuits to [].
- `testHistoryUnsubscribesAfterCollecting` - Asserts history() tears down its deliver subscription (UNSUB 2) once the replay of the single pending revision completes.
- `testGetAllReturnsEveryLiveKeyNotJustTheFirst` - Asserts getAll() returns every live key from the stream state (username and email), not just the first one.
- `testStreamInfoRequestSubjectAndPayloadAreExact` - Asserts the STREAM.INFO request is published to $JS.API.STREAM.INFO.KV_cfg with an exact "subjects_filter":"$KV.cfg.>" and a first-page "offset":0.
- `testGetStatusCastsCountersToIntAndPrefersLastSeq` - Asserts getStatus() casts float counters to int and reports last_sequence from last_seq (12) in preference to messages (7).
- `testGetStatusDefaultsMissingCountersToZero` - Asserts messages, last_sequence and bytes each default to exactly 0 when the stream state carries none of them.
- `testWatchCaughtUpFiresOnceForEmptyBucketThenDelivery` - Asserts onCaughtUp fires once at consumer creation for an empty bucket and the one-shot latch keeps a later delivery from re-firing it.

### tests/Unit/Mutation/KeyValueBucket_3MutationTest.php
- `testStreamInfoErrorCodeDefaultsToZeroWhenCodeAbsent` - Asserts a STREAM.INFO error envelope with no "code" surfaces as a JetStreamException whose code is exactly 0, with the server description preserved.
- `testStreamInfoErrorUsesProvidedCodeNotZero` - Asserts a STREAM.INFO error carrying "code":500 surfaces that code rather than the zero default.
- `testPaginationOffsetAccumulatesAcrossPages` - Asserts the paginated STREAM.INFO loop adds each page's subject count to the running offset, so the three requests carry offsets 0, 1 and 2.
- `testPaginationStopsWhenPageAddsNoNewSubjects` - Asserts the page loop stops after a non-empty page that adds no new subjects, issuing exactly three STREAM.INFO requests and never consuming a fourth page.
- `testDecodeReplyAcceptsJsonAtDepthLimit` - Asserts a reply nested right at the depth-512 json_decode limit is accepted rather than rejected as malformed.
- `testDecodeReplyRejectsJsonBeyondDepthLimit` - Asserts a reply nested beyond the depth-512 limit is rejected with JetStreamException "Malformed JetStream reply".
- `testMalformedReplyMessageIsPrefixThenCause` - Asserts the malformed-reply message starts with "Malformed JetStream reply: " and then carries the underlying "Syntax error" cause.
- `testMalformedReplyExceptionCodeIsZero` - Asserts the malformed-reply JetStreamException carries code exactly 0.
- `testCreateKeyRecognisesWrongLastSequenceByErrCode10071` - Asserts createKey() recognises the wrong-last-sequence rejection by err_code 10071 alone, even when the description shares no wording with it, and goes on to recreate successfully (#154).
- `testPublishErrorDescriptionIsCoercedToString` - Asserts a numeric error.description is coerced to a string before reaching the exception constructor, so the message is "12345" instead of a TypeError.
- `testPublishErrorCodeDefaultsToZeroWhenAbsent` - Asserts a publish error with no "code" yields exception code exactly 0.
- `testPublishErrorUsesProvidedCodeNotZero` - Asserts a publish error carrying "code":503 surfaces that code rather than the zero default.
- `testPublishErrorCodeIsCoercedToInt` - Asserts a string error.code ("503") is cast to int before reaching the exception constructor, yielding exception code 503 instead of a TypeError.

### tests/Unit/Mutation/KeyValueBucket_4MutationTest.php
- `testKeysDrainsChunkedRecordWithoutIdleSleepPerChunk` - Asserts keys() pays no 1 ms idle sleep on a read that consumed bytes, so a record arriving as ~360 one-byte chunks drains well inside a 0.2 s progress bound and returns ['email'] instead of timing out (#119).
- `testHistoryThrowsOnStalledReplayAndStillUnsubscribes` - Asserts a history() replay that makes no progress fails with a stalled JetStreamException quoting the caller's 0.05 s bound and still writes UNSUB 2 on that throw path (#119).

### tests/Unit/Mutation/KeyValueBucket_5MutationTest.php

- `testWatchInHandlerCaughtUpLatchFiresOnlyOnceAcrossDeliveries` - watch()'s in-handler end-of-initial-data latch is one-shot: on a consumer created with num_pending:1 (so the immediate path stays silent), two deliveries that each report num_pending:0 are both handled but onCaughtUp fires exactly once.
- `testGetStatusCastsFloatLastSeqToInt` - getStatus() coerces a JSON float last_seq (12.0) with (int) so last_sequence is returned as the integer 12, not a float.
- `testWatchImmediateCaughtUpLatchSuppressesLaterDeliverySignal` - watch()'s immediate end-of-initial-data signal on an empty bucket (num_pending:0 at consumer creation) latches, so a later live delivery that also reports pending 0 is handled without re-firing the one-shot onCaughtUp (#99).

### tests/Unit/Mutation/KeyWatchOptionsMutationTest.php
- `testIgnoreDeletesDefaultsToFalse` - Asserts ignoreDeletes defaults to false, so a default watcher still receives delete and purge tombstones unless the caller opts out.
- `testMetaOnlyDefaultsToFalseAndOmitsHeadersOnly` - Asserts metaOnly defaults to false and that the resolved consumer config therefore omits headers_only, so value bytes are delivered by default.
- `testMetaOnlyTrueEmitsHeadersOnly` - Asserts an explicit metaOnly emits headers_only:true in the consumer config, anchoring the positive side of that default.
- `testConfigAlwaysSetsAckPolicyNone` - Asserts toConsumerConfig() always seeds ack_policy "none", both for a default instance and on the resumeFromRevision branch that never sets it itself.

### tests/Unit/Mutation/MessageTtlMutationTest.php
- `testAcceptsIntegerOneSecondAtBoundary` - Asserts the integer TTL boundary is inclusive at 1, so format(1) returns "1s" rather than being rejected as too small.
- `testAcceptsIntegerStringOneSecondAtBoundary` - Asserts the bare-integer string "1" is likewise accepted at the boundary and formatted as "1s".
- `testNonNumericPrefixWithTrailingDigitsPassesThrough` - Asserts the bare-integer regex is anchored at the start, so "abc5" is not treated as a number and is passed through to the server verbatim.
- `testZeroDurationWithTrailingJunkPassesThrough` - Asserts the zero-duration regex is anchored at the end, so "0sX" is not recognised as a zero duration and is passed through verbatim.
- `testFullyZeroDurationStillRejected` - Asserts a fully zero duration ("0s") is still rejected with JetStreamException "Per-message TTL must be at least 1 second".

### tests/Unit/Mutation/NatsClientMutationTest.php
- `testSubscribeQueueBuffersEarlyDeliveryUntilQueueConstructed` - Asserts a message dispatched to a subscribeQueue() handler before its SubscriptionQueue exists is buffered and replayed into the queue once constructed, so the early delivery is fetchable rather than dropped (#129).
- `testFlushIsPubliclyCallableAndRoundTripsPing` - Asserts flush() is callable from outside the class and actually writes a PING control frame that round-trips against the seeded PONG.

### tests/Unit/Mutation/NatsConnection_1MutationTest.php
- `testMaxPayloadReturnsNullBeforeServerInfoIsKnown` - Asserts maxPayload() yields null before connect, so the null-safe read of the not-yet-known server info never dereferences it.
- `testRttThrowsWhenNotOpenAndFlushesAPingWhenOpen` - Asserts rtt() throws ConnectionException "Connection is not open" on a never-opened connection and, when open, drives flush() so a third PING (after CONNECT and the handshake PING) reaches the wire.
- `testAuthFailureEmitsClosedEvent` - Asserts a handshake that fails with an authorization -ERR still emits the Closed lifecycle event to the connection listener.
- `testReconnectEnabledWithZeroAttemptsDoesNotRecoverAndThrows` - Asserts that with reconnect enabled but maxReconnectAttempts at the boundary value 0 a failed connect never enters recovery and surfaces the ConnectionException.
- `testRetryInitialConnectWithZeroAttemptsDoesNotRetryAndThrows` - Asserts that with retryOnFailedInitialConnect enabled and maxReconnectAttempts 0 the failed initial connect throws after exactly one transport connect attempt, with no retry.
- `testGeneralConnectFailureEmitsClosedEvent` - Asserts a general connect failure (bad control line, reconnect and retry disabled) emits the Closed lifecycle event before the ConnectionException propagates.
- `testDisconnectCancelsPingTimer` - Asserts disconnect() cancels the heartbeat timer armed while open, clearing the timer id so no repeating timer leaks.
- `testDrainLatchesCloseIntentCancelsTimerWritesPingAndLeavesNoPongSlot` - Asserts drain() latches close-intent so a mid-drain recovery cannot re-open, cancels the ping timer, writes a flush PING, leaves no pong correlation slot behind, and ends Closed (#117).
- `testDrainDeliversBufferedMessagesNotDrainedByTheFlushLoop` - Asserts drain() hands a still-buffered message to its subscription callback after the flush loop ends, so a message the loop never read is not silently lost.
- `testBufferedPublishRecordsOutboundExactlyOnceAndDoesNotWriteImmediately` - Asserts a publish issued while a reconnect is in flight records its outbound message and bytes exactly once and holds the frame in the reconnect buffer instead of writing it to the transport.
- `testPublishWithHeadersValidatesSubject` - Asserts publishWithHeaders() rejects a subject containing whitespace with a ProtocolException before any frame is built or sent.
- `testZeroReconnectBufferSizeDisablesBufferingDuringReconnect` - Asserts a reconnectBufferSize of 0 disables buffering entirely, so a publish during reconnect throws ConnectionException "Connection is not open" rather than being swallowed.
- `testBufferFrameAcceptsFrameThatExactlyFillsTheBuffer` - Asserts a frame whose length exactly equals reconnectBufferSize is still buffered rather than rejected as overflow, pinning the inclusive buffer-full boundary.
- `testRecordOutboundAccumulatesBytesAcrossPublishes` - Asserts outbound byte statistics accumulate across publishes (3 plus 7 bytes reported as 10) instead of being overwritten by the last payload.
- `testDrainSubscriptionDropsLocalStateWhenNotOpen` - Asserts drainSubscription() on a not-open connection drops the subscription's local metadata and handler entries.
- `testDrainSubscriptionDeliversBufferedMessageWhenFlushTimesOut` - Asserts drainSubscription() delivers a message buffered for that sid even when its flush times out, via the explicit pending drain.
- `testFlushThrowsWhenNotOpenAndWritesPingWhenOpen` - Asserts flush() throws ConnectionException "Connection is not open" without writing any bytes when not open, and writes a PING when the connection is open.
- `testFlushTimeoutLeavesItsPongSlotQueuedForItsLatePong` - Asserts a timed-out flush leaves its incomplete pong slot at the head of the correlation queue so its late PONG is attributed to that ping instead of releasing the next flush a pong early (#117).
- `testFlushLeavesNoPongSlotAfterSuccessfulFlush` - Asserts a successful flush consumes its correlation slot so nothing lingers in the pong waiter queue (#117).
- `testProcessIncomingReturnsZeroWhenReadAlreadyInProgress` - Asserts processIncoming() short-circuits to 0 without consuming the queued frame while another read owns the socket, and dispatches that frame once the guard is released.
- `testProcessIncomingMarksReadInProgressToExcludeConcurrentRead` - Asserts the first processIncoming() latches the in-progress flag so a concurrent call returns 0 immediately instead of entering a second blocking read.
- `testReadFailureEmitsErrorToListener` - Asserts a transport read failure is reported to the error listener before recovery runs.
- `testEmptyReadDoesNotDeliverBufferedMessages` - Asserts an empty read returns 0 and leaves buffered messages undelivered, so no message is dispatched on a read that produced no bytes.

### tests/Unit/Mutation/NatsConnection_2MutationTest.php
- `testRequestValidatesSubjectBeforeSubscribing` - Asserts request() rejects an empty subject with a ProtocolException before any inbox subscription is created, so no SUB frame reaches the wire.
- `testRequestWithHeadersValidatesSubjectBeforeSubscribing` - Asserts requestWithHeaders() rejects an empty subject with a ProtocolException before writing any SUB frame.
- `testRequestManyValidatesSubjectBeforeSubscribing` - Asserts requestMany() rejects an empty subject with a ProtocolException before writing any SUB frame.
- `testRequestTimeoutMessageIncludesPrefixThenSubject` - Asserts a request timeout reports exactly "Request timed out for subject svc.echo", pinning both the prefix and the appended subject in that order.
- `testRequestManyStopsImmediatelyOnNoRespondersSentinel` - Asserts a 503 no-responders sentinel short-circuits requestMany() to an empty result immediately rather than running out the full total budget.
- `testRequestManyStopsExactlyAtMaxResponses` - Asserts requestMany() stops as soon as the collected count reaches maxResponses, returning the single reply well before the total budget elapses.
- `testRequestManyDoesNotStallBeforeStallIntervalElapses` - Asserts a generous stall interval does not fire between two genuinely quick replies, so both payloads are collected in order.
- `testRequestManyStopsShortlyAfterStallInterval` - Asserts requestMany() ends collection shortly after the stall interval expires on an idle socket, returning the one collected reply instead of waiting out the total budget.
- `testRequestManyReturnsCollectedSetWhenTotalElapsesWithUnrequestedExternalCancellation` - Asserts that when the total budget elapses with an external cancellation supplied but never requested, requestMany() returns the collected set instead of throwing.

### tests/Unit/Mutation/NatsConnection_3MutationTest.php
- `testNoRespondersRequiresStatusAtStartOfLine` - Asserts a header whose first line merely contains "NATS/1.0 503" without starting with it is treated as a normal reply and collected, since the no-responders check is anchored to the start of the line.
- `testNoRespondersStatusReturnsEmpty` - Asserts a genuine "NATS/1.0 503" status line is recognised as the no-responders sentinel so requestMany() returns an empty result.
- `testUrlTokenCredentialEncodedAsAuthToken` - Asserts a lone userinfo component in the server URL is sent in CONNECT as an auth_token, with no user or pass fields.
- `testTlsRequiredButInactiveThrowsExactMessage` - Asserts a connection whose TLS handshake never activated fails with the exact ConnectionException text naming the missing TLS materials, both halves of the message in order.
- `testSeedDiscoveredServersRequiresNonEmptyConnectUrls` - Asserts a reconnect to a server whose INFO carries no connect_urls retains the previously discovered peers instead of overwriting them with an empty set.
- `testRequestManyTotalTimeoutWithoutExternalCancellationReturnsCollected` - Asserts a per-slice read timeout with no external cancellation is swallowed so requestMany() keeps looping and returns the collected reply at the total deadline.
- `testRequestManyTotalTimeoutWithUnrequestedExternalCancellationReturnsCollected` - Asserts a per-slice read timeout is still swallowed when an external cancellation is present but never requested, so the collected reply is returned rather than the timeout rethrown.
- `testRetryInitialConnectAttemptCountIsExact` - Asserts retryInitialConnect with maxReconnectAttempts 1 performs exactly one initial connect plus one retry (two transport connect calls) before throwing.
- `testRetryInitialConnectRethrowsAuthError` - Asserts retryInitialConnect aborts immediately with an AuthenticationException on an auth failure instead of swallowing it and retrying to exhaustion.
- `testRetryInitialConnectEmitsConnectedOnSuccess` - Asserts a retry that succeeds after a failed initial connect leaves the connection Open and emits the Connected lifecycle event.
- `testPerformRecoveryEmitsClosedWhenReconnectDisabled` - Asserts recovery with reconnect disabled emits Closed and leaves the connection Closed before throwing "Reconnect is disabled".
- `testPerformRecoveryBailsWhenClosingWithoutDisconnectEvent` - Asserts recovery returns immediately when close-intent is set, emitting no Disconnected event and dialling no second server.
- `testRecoverConnectionEarlyReturnSkipsPendingDrain` - Asserts recoverConnection bails at the top while close-intent is set, so a buffered message is never delivered by the post-recovery drain.
- `testRecoverConnectionAwaitsInProgressReconnect` - Asserts recoverConnection awaits an already in-progress reconnect and surfaces its error to the caller rather than returning normally.
- `testReconnectingClearedAfterFailedRecovery` - Asserts the reconnecting slot is cleared by the finally clause even when recovery exhausts every attempt and rethrows, so no errored future lingers.
- `testRecoveryClosesTransportBeforeReconnect` - Asserts recovery closes the dead transport before dialling again, with the reconnect succeeding and the connection returning to Open.
- `testRetryInitialConnectClosesTransportBetweenAttempts` - Asserts retryInitialConnect closes the transport before each retry attempt, with the successful retry leaving the connection Open.
- `testPerformRecoveryCancelsPingTimer` - Asserts recovery cancels the active ping watchdog before reconnecting, so a recovery that exhausts every attempt leaves no armed timer behind.
- `testPerformRecoveryMakesAtLeastOneAttemptWhenMaxIsZero` - Asserts recovery reached from a read failure still makes exactly one reconnect attempt when maxReconnectAttempts is 0.
- `testPerformRecoveryAttemptCountIsExactWhenMaxIsOne` - Asserts recovery makes exactly one reconnect attempt when maxReconnectAttempts is 1, never two.

### tests/Unit/Mutation/NatsConnection_4MutationTest.php
- `testReconnectAuthFailureNotifiesErrorListener` - Asserts a reconnect handshake that fails with an auth error notifies the error listener (alongside the triggering read failure) before Closed is emitted and the AuthenticationException is rethrown.
- `testReconnectExhaustionEmitsClosedEvent` - Asserts recovery that exhausts every reconnect attempt emits the Closed lifecycle event and surfaces a ConnectionException.
- `testReconnectExhaustionThrowsWithZeroCode` - Asserts the exhaustion failure is exactly ConnectionException "Reconnect attempts exhausted" with code 0.
- `testReconnectWarningLogsAttemptAndDelay` - Asserts each failed reconnect attempt logs a warning whose context carries the attempt number, the max attempt count and the computed backoff delay (1ms for the first attempt at reconnectDelayMs 1).
- `testBackoffFloorsBaseAtOne` - Asserts a reconnectDelayMs of 0 is floored to a 1ms backoff base, as reported by the logged delayMs.
- `testReconnectWithEmptyBufferWritesNothingExtra` - Asserts a clean reconnect with nothing buffered writes no empty frame, because the reconnect-buffer flush early-returns.
- `testDrainProcessesMsgAfterOkInSameChunk` - Asserts the reconnect replay drain skips a +OK frame and still processes a MSG co-chunked after it, so that message is delivered.
- `testDrainStopsOnEmptyChunkLeavingLaterMsgForNextRead` - Asserts the reconnect replay drain stops at an empty chunk, leaving a later queued MSG undelivered until the next processIncoming() picks it up.
- `testHandshakeContinuesPastEmptyChunkBeforePong` - Asserts the initial-PONG wait continues past an empty handshake chunk and keeps polling, so the connection reaches Open.
- `testHandshakeContinuesAfterInfoToReachPongInSameChunk` - Asserts the initial-PONG wait applies an INFO update and continues to the PONG in the same chunk, completing the handshake.
- `testAwaitServerInfoContinuesAfterPingToReachInfoInSameChunk` - Asserts the server-info wait answers a server PING with a PONG and continues to the INFO in the same chunk, so connect completes with the server id recorded.

### tests/Unit/Mutation/NatsConnection_5MutationTest.php
- `testHandshakePollBudgetIsSixteenAtConnectTimeout150` - Asserts a doomed handshake at connectTimeoutMs 150 performs exactly 16 reads before failing with "Expected INFO during connect", pinning the 16-poll floor of the handshake poll budget.
- `testHandshakePollBudgetIsTwentyOneAtConnectTimeout203` - Asserts a doomed handshake at connectTimeoutMs 203 performs exactly 21 reads, pinning the divisor of 10 and the ceiling rounding of the handshake poll budget.
- `testNonRecoverableServerErrorFrameThrowsWithErrorTextAppended` - Asserts a non-recoverable server -ERR frame throws a ConnectionException starting with "Server sent error frame: " and carrying the verbatim server error text.
- `testRecoverableServerErrorFrameMessageStartsWithStaticPrefix` - Asserts a recoverable permissions -ERR frame is reported to the error listener as "Server sent recoverable error frame: " plus the error text, with the connection staying Open.
- `testMalformedAsyncInfoEmitsPrefixedErrorWithJsonDetail` - Asserts a malformed async INFO frame emits exactly one listener error prefixed "Discarding malformed async INFO frame: " with the JSON detail appended, leaving the read loop and connection intact.
- `testInBytesAccumulatesAcrossMessages` - Asserts inbound byte statistics accumulate across messages (5 plus 6 reported as 11 over 2 messages) rather than being overwritten by the last one.
- `testHmsgWithZeroHeaderBytesHasNullRawHeaders` - Asserts an HMSG declaring 0 header bytes yields a message with null rawHeaders and the full body as payload, not an empty-string header block.

### tests/Unit/Mutation/NatsConnection_6MutationTest.php
- `testHmsgWithZeroHeaderBytesYieldsNullRawHeaders` - Asserts an HMSG declaring 0 header bytes is treated as having no headers, delivering the payload with rawHeaders null rather than an empty header block.
- `testDropOldestEmitsExactMessageWithSid` - Asserts the DropOldest slow-consumer policy reports exactly "Slow consumer on sid 1: dropped oldest message" and keeps the newer message.
- `testDropNewestEmitsExactMessageWithSid` - Asserts the DropNewest slow-consumer policy reports exactly "Slow consumer on sid 1: dropped newest message" and keeps the earlier message.
- `testOverflowErrorPolicyThrowsExactMessageWithoutDoubleEmitting` - Asserts the Error slow-consumer policy throws ConnectionException "Subscription queue overflow for sid 1" as the single surfacing point, with the error listener left untouched so the overflow is not reported twice (#159/#158).
- `testInboundFrameAtExactNegotiatedBoundIsAccepted` - Asserts a MSG whose size lands exactly on the negotiated inbound bound (server max_payload plus 1 MiB) is parsed and delivered with the connection staying Open.
- `testInboundFrameOneByteOverNegotiatedBoundIsRejected` - Asserts a MSG one byte over the negotiated inbound bound is rejected, delivering nothing and surfacing a ConnectionException with reconnect disabled.
- `testZeroMaxPayloadUsesLargeFallbackBoundAndAcceptsAboveEightMiB` - Asserts a server advertising max_payload 0 makes the inbound bound the large fallback, so a frame just over 8 MiB is still accepted and delivered.
- `testPublishNotEnforcedWhenServerMaxPayloadIsZero` - Asserts outbound size enforcement is skipped when the server advertises max_payload 0, so a non-empty publish succeeds and writes its PUB frame.
- `testHandshakeAcceptsInfoNested511DeepUnderDepthLimit` - Asserts a handshake INFO nested 511 levels deep parses under the 512 JSON depth limit and the connection reaches Open.
- `testHandshakeRejectsInfoNested512DeepAtDepthLimit` - Asserts a handshake INFO nested 512 levels deep exceeds the JSON depth limit and aborts the connect instead of reaching Open.
- `testAuthConnectErrorMessageIsExactAndInOrder` - Asserts an authorization -ERR during connect throws an AuthenticationException reading exactly "Server rejected authentication during connect: Authorization Violation".
- `testServerConnectErrorMessageIsExactAndInOrder` - Asserts a non-auth -ERR during connect throws a plain ConnectionException reading exactly "Server error during connect: Maximum Connections Exceeded".

### tests/Unit/Mutation/NatsConnection_7MutationTest.php
- `testConnectedEventIsLoggedWithEventContextKey` - Asserts a successful connect logs the Connected lifecycle event once at info level as "NATS connection Connected" with a context array carrying 'event' => 'Connected'.
- `testClosedWithErrorIsLoggedAsWarningWithFullMessageAndContext` - Asserts a lifecycle event carrying an error (an auth failure during connect) is logged at warning level with the exact message "NATS connection Closed", the 'event' context key and the throwable under 'exception'.
- `testEmitErrorLogsPrefixedMessageWithExceptionContext` - Asserts a recoverable server -ERR is logged exactly once as the "NATS connection error: " prefix followed by the throwable message, with the throwable in the 'exception' context, while the connection stays open.
- `testDiscoveredServersNotReemittedForEmptyConnectUrls` - Asserts DiscoveredServers fires only when the advertised connect_urls are both non-empty and changed, so a later async INFO with no connect_urls does not re-fire the event.
- `testLameDuckAnnouncedOnlyOnce` - Asserts the lame-duck announcement latches, so two consecutive ldm INFO frames emit exactly one LameDuck event.
- `testLameDuckDoesNotFailoverWithSingleServerPool` - Asserts lame-duck auto-failover only triggers when the server pool holds more than one endpoint, so a single-server pool performs no second dial on an ldm INFO.
- `testWildcardTokenStillValidatesLaterTokens` - Asserts subject validation keeps checking the remaining tokens after a standalone wildcard token, so '*.bad*' is still rejected with ProtocolException "Wildcards must occupy an entire token".
- `testDispatchGuardClearedAfterHandlerThrows` - Asserts the per-sid dispatch guard is cleared even when a handler throws, so a later message for the same sid is still delivered instead of the subscription wedging permanently.

### tests/Unit/Mutation/NatsConnection_8MutationTest.php
- `testReadInProgressReportsIdleNotConsumed` - Asserts a readIncoming() skipped because another fiber already owns the socket read reports 0 frames and consumedBytes=false, so a wait loop idle-yields instead of busy-spinning on wire progress it never made.
- `testReadErrorReportsNotConsumedAfterRecovery` - Asserts a read that fails with a mid-session EOF triggers a reconnect, leaves the connection Open, and reports 0 frames with consumedBytes=false because a failed read landed no data.
- `testParseErrorReportsConsumedBytesAfterRecovery` - Asserts a read that pushed a non-empty but corrupt chunk triggers a reconnect and still reports consumedBytes=true, since those bytes genuinely came off the wire even though no frame parsed (#119).
- `testRequestManyDrainsChunkedReplyWithoutIdleSleepPerPartialChunk` - Asserts the requestMany() wait loop only idle-sleeps on a genuinely idle read, so a 300-byte reply delivered as one-byte chunks reassembles byte-for-byte inside a 60 ms request budget rather than paying 1 ms per partial chunk (#119).

### tests/Unit/Mutation/NatsConnection_9MutationTest.php
- `testAutoUnsubscribeCompletesWhenServerMaxAlreadyReached` - Asserts unsubscribe() with a max that has already been reached at intake and an empty local backlog drops the subscription immediately, so a reconnect cannot re-arm it and over-deliver (#112).
- `testPlainUnsubscribeOnNonOpenConnectionWritesNoUnsub` - Asserts a plain unsubscribe on a connection that is not Open releases local state and returns without writing an UNSUB frame onto the unusable socket (#116).
- `testRetryInitialConnectAuthFailureEmitsClosedEvent` - Asserts an AuthenticationException hit during an initial-connect retry emits a Closed lifecycle event before rethrowing, so the terminal close is observable rather than silent (#56).
- `testRetryInitialConnectExhaustionEmitsClosedEvent` - Asserts exhausting every initial-connect retry attempt reports failure so the connect path takes its terminal close and emits a Closed event alongside the ConnectionException.
- `testRecoveryReconnectDisabledReportsDiscardedInboundBacklog` - Asserts a terminal recovery on a reconnect-disabled connection names its discarded inbound backlog through the error listener as "2 parsed inbound message(s) were discarded undelivered" instead of dropping it silently (#123).
- `testResubscribeDropsSatisfiedSubscription` - Asserts the reconnect replay drops a subscription whose auto-unsubscribe max was already reached rather than re-SUBbing it and resetting the server counter (#112).
- `testResubscribeContinuesPastSatisfiedSubscriptionToReplayLaterSubs` - Asserts the reconnect replay continues past a dropped satisfied subscription and still writes "SUB live 2" for the later live subscription instead of aborting the whole replay.

### tests/Unit/Mutation/NatsHeadersMutationTest.php
- `testToWireBlockEmitsValueVerbatim` - Asserts `toWireBlock` emits the caller's value bytes untouched (`X-Pad:  v  \r\n`, nats.go parity) and never the trimmed `X-Pad:v` form, so any mutation that rewrites the value on encode changes the wire bytes.
- `testFromWireBlockMultiRequiresStatusLineAnchoredAtStart` - Asserts `fromWireBlockMulti` only recognises a status line anchored at the START of the first line: a first line reading `garbage NATS/1.0 503 Down` yields [] with no synthesised `Status` key.
- `testFromWireBlockMultiRequiresStatusLineAnchoredAtEnd` - Asserts `fromWireBlockMulti` requires the status regex to match to the END of the first line: a first line carrying a bare LF (`NATS/1.0 503 Desc\nExtra`) is not a status line, so the result is [] with no `Status` key.
- `testFromWireBlockMultiTrimsStatusDescription` - Asserts `fromWireBlockMulti` trims trailing whitespace from a status description, so `NATS/1.0 503 No Responders   ` yields `Description`=>['No Responders'].
- `testFromWireBlockMultiTrimsHeaderName` - Asserts `fromWireBlockMulti` trims whitespace around a header name, keying `  Spaced  :payload` as `Spaced`=>['payload'] and never as the untrimmed name.
- `testFromWireBlockMultiTrimsHeaderValue` - Asserts `fromWireBlockMulti` trims whitespace around a header value, parsing `MName:  mval  ` as `MName`=>['mval'].
- `testFromWireBlockRequiresStatusLineAnchoredAtStart` - Asserts `fromWireBlock` only recognises a status line anchored at the START of the first line: `garbage NATS/1.0 503 Down` yields [] with no synthesised `Status` key.
- `testFromWireBlockRequiresStatusLineAnchoredAtEnd` - Asserts `fromWireBlock` requires the status regex to match to the END of the first line: a first line containing a bare LF (`NATS/1.0 503 Desc\nExtra`) yields [] with no `Status` key.
- `testFromWireBlockTrimsStatusDescription` - Asserts `fromWireBlock` trims trailing whitespace from a status description, so `NATS/1.0 503 No Responders   ` yields `Description`=>'No Responders'.
- `testFromWireBlockStopsAtEmptyLineRatherThanContinuing` - Asserts `fromWireBlock` STOPS parsing at the blank line ending the header block rather than continuing into the body, returning exactly `['Before' => 'yes']` with no `After` key leaking in from the payload bytes.
- `testFromWireBlockTrimsHeaderName` - Asserts `fromWireBlock` trims whitespace around a header name, keying `  Spaced  :payload` as `Spaced`=>'payload' and never as the untrimmed name.

### tests/Unit/Mutation/NkeySeedSignerMutationTest.php
- `testLowercaseSeedIsUppercasedBeforeDecoding` - Asserts a lowercased seed is upper-cased before base32 decoding, so it is accepted and derives the canonical public key instead of failing on characters outside the base32 alphabet.
- `testSurroundingWhitespaceIsTrimmedBeforeDecoding` - Asserts leading and trailing whitespace is trimmed off the seed before decoding, so a padded seed still derives the canonical public key.
- `testSeedSecondPrefixByteIsShiftedRightByThree` - Asserts the second seed prefix byte is masked and shifted right by exactly three bits, so a valid cluster seed resolves to the CLUSTER prefix and yields a base32 public key starting with 'C'.
- `testDecodeAcceptsExactlyFourBytesBeforeChecksumStrip` - Asserts the decoder's minimum-length guard rejects only buffers shorter than 4 bytes, so an exactly 4-byte decode passes through and fails later as "Invalid NKey seed encoding" rather than as an encoding error.
- `testOperatorPrefixSeedIsAccepted` - Asserts an operator seed is accepted as a valid public prefix and derives a public key starting with 'O'.
- `testServerPrefixSeedIsAccepted` - Asserts a server seed is accepted as a valid public prefix and derives a public key starting with 'N'.
- `testClusterPrefixSeedIsAccepted` - Asserts a cluster seed is accepted as a valid public prefix and derives a public key starting with 'C'.
- `testAccountPrefixSeedIsAccepted` - Asserts an account seed is accepted as a valid public prefix and derives a public key starting with 'A'.

### tests/Unit/Mutation/NkeySeedSigner_1MutationTest.php
- `testCurvePrefixSeedIsAccepted` - Asserts a curve seed is accepted as a valid public prefix and derives a base32 public key starting with 'X'.
- `testSeedSecondPrefixByteMaskRetainsBitThree` - Asserts the 0xF8 mask applied to the second seed prefix byte keeps bit 3, so a seed whose byte carries that bit resolves to an odd prefix and is rejected with "Invalid NKey seed prefix" rather than being read as a cluster seed.

### tests/Unit/Mutation/ObjectInfoMutationTest.php
- `testMetadataValuesAreStringCoerced` - Asserts fromArray() coerces every metadata value to a string, so an int 7 becomes '7' and a bool true becomes '1'.
- `testLinkArrayWithoutBucketKeyIsNotALink` - Asserts a link array missing its bucket key produces no link at all, since both the array check and the bucket-key presence must hold.
- `testLinkBucketIsStringCast` - Asserts a well-formed link array yields a link whose bucket value is string-cast, so an int 12345 is exposed as '12345'.
- `testLinkNameIsStringCast` - Asserts a present non-empty link name is string-cast, so an int 999 is exposed as '999' alongside the link bucket.
- `testBucketFromDataTakesPrecedenceOverArgument` - Asserts the bucket from the meta record wins over the bucket argument passed to fromArray().
- `testMissingSizeDefaultsToZero` - Asserts a meta record with no size yields size exactly 0.
- `testMissingChunksDefaultsToZero` - Asserts a meta record with no chunks yields chunks exactly 0.
- `testModifiedReadsMtimeFromData` - Asserts the modified timestamp is read from the record's mtime field rather than defaulting to an empty string.
- `testMissingDeletedDefaultsToFalse` - Asserts a meta record with no deleted flag yields deleted false.

### tests/Unit/Mutation/ObjectStoreBucket_1MutationTest.php
- `testCreateSerializesDefaultDescriptionAndDirectAndSubjects` - Asserts create() serializes the exact default stream config on STREAM.CREATE: description "Object Store bucket assets", allow_direct true, and both chunk and meta subjects in order (`$O.assets.C.>`, `$O.assets.M.>`).
- `testSealDropsEmptyArrayConfigFields` - Asserts seal() filters empty-array config fields (consumer_limits) out of the STREAM.UPDATE while preserving scalar config (max_bytes) alongside sealed:true, so the server never receives an empty JSON array where an object is expected.
- `testAddLinkValidatesNameAndUsesExplicitTargetBucket` - Asserts addLink() with an explicit target bucket records that bucket (not the current one) in the link record and resolves the target's info against the OTHER bucket's stream and meta subject, never touching this bucket's stream.
- `testAddLinkRejectsEmptyName` - Asserts addLink() with an empty object name throws JetStreamException "Invalid object name".
- `testAddBucketLinkRejectsEmptyName` - Asserts addBucketLink() with an empty object name throws JetStreamException "Invalid object name".
- `testLinkMetaFieldsExactValues` - Asserts a link's meta record carries the exact field values: the link's own name, this bucket, size 0, chunks 0 and deleted false, both on the returned ObjectInfo and on the published meta frame.
- `testPutSmallObjectInfoFields` - Asserts put() of a payload at or below chunkSize publishes exactly one chunk and records bucket, deleted:false and options.max_chunk_size on the meta record.
- `testPutBoundaryExactChunkSizeIsSingleChunk` - Asserts put() of a payload exactly equal to chunkSize reports size and chunks 1 and emits a single chunk publish (the inclusive size boundary).
- `testPutEmptyDescriptionIsOmitted` - Asserts put() given an empty-string description omits the description field from the meta record entirely.
- `testPutNonEmptyDescriptionIsIncluded` - Asserts put() given a non-empty description serializes it as "description" on the meta record.
- `testPutStreamRejectsEmptyName` - Asserts putStream() with an empty object name throws JetStreamException "Invalid object name".
- `testPutStreamAccumulatesSizeAndBufferAcrossBlocks` - Asserts putStream() accumulates both the total size and the buffered bytes across multiple producer blocks, so the reported size is the sum and the tail chunk publishes the full concatenation rather than only the last block.
- `testPutStreamInfoCarriesNameField` - Asserts putStream()'s meta record and returned ObjectInfo carry the object name.
- `testPutStreamChunkBoundaryAndSubstr` - Asserts putStream() splits a block that is an exact multiple of chunkSize into whole chunks of exactly chunkSize taken at the running offset, published in stream order with the digest of the full payload.
- `testPutPipelineFlushesAtDepthBoundary` - Asserts put() at exactly the upload pipeline depth (16 chunks) publishes every chunk with the full-payload size, chunk count and digest, dropping none at the flush boundary.
- `testPutStreamPipelineFlushesAtDepthBoundary` - Asserts putStream() at exactly the upload pipeline depth (16 chunks) publishes every chunk with the full-payload size, chunk count and digest via the streaming path.

### tests/Unit/Mutation/ObjectStoreBucket_2MutationTest.php
- `testPutStreamMetaRecordCarriesBucketDeletedFalseAndChunkSize` - Asserts putStream()'s final meta record carries the bucket key, deleted:false and an options.max_chunk_size entry matching the configured chunk size, with the returned ObjectInfo also reporting deleted false.
- `testGetResolvesExactlyEightLinkHopsToRealObject` - Asserts get() starts link resolution at depth 0, advances exactly one hop at a time and treats the eight-hop ceiling as inclusive, so a chain of exactly eight links resolves to the real object and returns its data.
- `testGetTooManyHopsMessageIsExact` - Asserts a link chain past the hop ceiling makes get() throw JetStreamException reading exactly `Too many Object Store link hops resolving "loop.txt"`.
- `testGetFollowsCrossBucketObjectLinkToOtherBucket` - Asserts get() follows a cross-bucket object link into the target bucket, resolving the target's meta and chunk against OBJ_other instead of the current bucket.
- `testGetBucketLinkMessageIsExact` - Asserts get() of a bucket link (a link carrying no object name) throws JetStreamException reading exactly `Cannot get() the bucket link "bucket-link": it points to a bucket, not an object`.
- `testGetToCallbackResolvesExactlyEightLinkHops` - Asserts the streaming getToCallback() path applies the same inclusive eight-hop ceiling, resolving a chain of exactly eight links and streaming the target object's bytes to the callback.
- `testGetToCallbackTooManyHopsMessageIsExact` - Asserts getToCallback() on an over-long link chain throws the same exact `Too many Object Store link hops resolving "loop.txt"` message.
- `testGetToCallbackVerifiesDigestAfterStreaming` - Asserts getToCallback() still verifies the digest after streaming, so a multi-chunk object with the expected chunk count but corrupted content throws "Object digest mismatch" rather than returning its info silently.
- `testGetEmptyObjectShortCircuitsWithoutEphemeralConsumer` - Asserts an object with zero chunks short-circuits to empty content without creating an ephemeral consumer, writing neither a CONSUMER.CREATE nor a CONSUMER.MSG.NEXT request.
- `testMultiChunkDownloadSetsDeliverPolicyAndDeletesConsumer` - Asserts a multi-chunk download creates its ephemeral consumer with deliver_policy "all" and deletes that named consumer once the download completes.
- `testNullConsumerNameSkipsDeletionOnCreateFailure` - Asserts a failed consumer create leaves the consumer name null so the cleanup guard skips deletion entirely and the original create error surfaces as a JetStreamException, with no CONSUMER.DELETE written.
- `testNonTimeoutFetchErrorStillDeletesConsumerViaFinally` - Asserts a non-timeout fetch error (a terminal 409) propagates to the caller while the finally block still deletes the ephemeral consumer.
- `testDigestMismatchMessageIsExact` - Asserts a corrupted single-chunk download throws with the exact message "Object digest mismatch: expected <stored>, got <computed>", both operands present and in order.

### tests/Unit/Mutation/ObjectStoreBucket_3MutationTest.php
- `testGetRejectsDigestWithoutSha256Prefix` - Asserts a stored digest lacking the "SHA-256=" prefix is rejected rather than decoded, so get() throws "Object digest mismatch" even though the body after the wrong prefix holds the correct hash bytes.
- `testInfoRejectsEmptyName` - Asserts info('') throws JetStreamException "Invalid object name" instead of letting the empty name reach a Direct Get request.
- `testInfoFallbackTreats404AsAbsent` - Asserts the leader STREAM.MSG.GET fallback taken after a Direct Get 503 treats a 404 as an absent object and returns null rather than rethrowing.
- `testInfoFallbackSurfacesRevisionFromSeq` - Asserts the fallback path surfaces the record's stream sequence as ObjectInfo::revision (42) instead of null.
- `testDeleteRejectsEmptyName` - Asserts delete('') throws JetStreamException "Invalid object name".
- `testDeleteTombstoneFieldsArePinned` - Asserts delete() publishes a tombstone meta record carrying the object name, the bucket, size 0, chunks 0 and the options.max_chunk_size entry.
- `testUpdateMetaRejectsEmptyName` - Asserts updateMeta('') throws JetStreamException "Invalid object name".
- `testUpdateMetaMissingObjectThrowsWith404Code` - Asserts updateMeta() on a missing object throws JetStreamException "Object not found" with code exactly 404.
- `testUpdateMetaRenameToEmptyNameIsRejected` - Asserts a rename to an empty new name is rejected with "Invalid object name" once the existing object has been found.
- `testUpdateMetaInPlaceMetaFieldsArePinned` - Asserts an in-place metadata update republishes the meta record with the existing bucket, chunks, digest and the options.max_chunk_size entry preserved.
- `testUpdateMetaRenameTombstoneFieldsArePinned` - Asserts a rename tombstones the old name with a record carrying that name, the bucket, size 0, chunks 0, the options.max_chunk_size entry and deleted:true.
- `testWatchFilterIsMetaPrefixThenPattern` - Asserts watch() builds its consumer filter as the meta prefix followed by the pattern, giving exactly "$O.assets.M.>" for the default pattern.

### tests/Unit/Mutation/ObjectStoreBucket_4MutationTest.php
- `testListSurfacesRevisionFromNatsSequenceHeader` - Asserts list() surfaces a meta record's Nats-Sequence header as the ObjectInfo revision (11) rather than null.
- `testListContinuesPastDeletedRecordToReturnLaterLiveOnes` - Asserts list() skips a deleted record and keeps iterating, so a live record positioned after it is still returned instead of the loop aborting.
- `testGetStatusCastsCountersToInt` - Asserts getStatus() coerces messages, last_sequence and bytes to real ints when the server reports them as JSON strings.
- `testPublishMetaErrorWithoutCodeDefaultsToZero` - Asserts a meta-publish error object carrying no code surfaces as a JetStreamException with code exactly 0 and the server description preserved.
- `testPublishMetaErrorUsesProvidedCode` - Asserts a meta-publish error that does carry a code surfaces that code (400) instead of flattening it to 0.
- `testListRequestsStreamInfoApiSubject` - Asserts the subject enumeration behind list() targets the full $JS.API.STREAM.INFO.OBJ_assets subject rather than a bare stream name.
- `testMetaSubjectsEnumerationErrorPropagatesProvidedCode` - Asserts an enumeration error's provided code (503) and description propagate through list() verbatim.
- `testMetaSubjectsEnumerationErrorWithoutCodeDefaultsToZero` - Asserts an enumeration error omitting the code surfaces with code exactly 0.
- `testPaginationStopsWhenPageAddsNoNewSubjects` - Asserts the subject pagination loop stops as soon as a page contributes no new subjects, issuing exactly two STREAM.INFO enumeration requests.
- `testRequestStreamMessageSendsLastBySubjFilter` - Asserts the STREAM.MSG.GET request body carries a last_by_subj filter naming the encoded meta subject, so the leader returns the latest meta record.
- `testRequestStreamMessageErrorCastsDescriptionAndCode` - Asserts a STREAM.MSG.GET error casts a non-string description to string and a string code to int, surfacing JetStreamException "1234" with code 500 instead of a TypeError.
- `testRequestStreamMessageErrorWithoutCodeDefaultsToZero` - Asserts a STREAM.MSG.GET error omitting the code surfaces with code exactly 0 and its description preserved.
- `testRequestStreamMessageReturnsFullResponseNotFirstItemOnly` - Asserts the whole decoded STREAM.MSG.GET response is returned, so a "message" key placed after another top-level key is still read and info() resolves the object with revision 9.
- `testNuidIs22HexChars` - Asserts a generated object NUID is exactly 22 lowercase hex characters, matching 11 random bytes.

### tests/Unit/Mutation/ObjectStoreBucket_5MutationTest.php
- `testFetchInfoFallbackCastsStringSeqToIntRevision` - Asserts the leader STREAM.MSG.GET fallback casts a string "seq" to int, so ObjectInfo::revision comes back as the integer 42 rather than raising a TypeError.
- `testRequestStreamMessageBodyCarriesLastBySubjFilter` - Asserts the STREAM.MSG.GET request frame itself carries a {"last_by_subj":"$O.assets.M.<enc>"} body rather than an empty array, so the leader is asked for the latest meta record.
- `testFetchInfoCastsNonStringDataFieldAndReturnsNull` - Asserts a non-string "data" field on the STREAM.MSG.GET record is cast to string before decoding, so info() cleanly resolves to null instead of raising a TypeError.

### tests/Unit/Mutation/ProtocolCodecMutationTest.php
- `testConnectAdvertisesLangAndProtocolAndHeaders` - Asserts the CONNECT payload advertises lang "php", protocol exactly 1, the caller's verbose and pedantic flags under their own keys, and headers:true.
- `testJwtBranchOmitsNkeyWhenNkeyIsNull` - Asserts the JWT CONNECT branch omits the nkey field entirely when no nkey is configured, never emitting "nkey":null.
- `testSeedSignerMismatchGuardDoesNotFireWithoutExplicitNkey` - Asserts the seed-signer/nkey mismatch guard stays quiet when a NkeySeedSigner is present but no explicit nkey is configured, so a CONNECT is produced with no nkey field instead of a spurious ProtocolException.
- `testConnectAdvertisesInstalledVersionNotFallback` - Asserts CONNECT advertises the package's installed Composer version rather than the in-source fallback constant whenever runtime metadata is available.
- `testHeaderPublishTotalBytesIsHeaderPlusPayload` - Asserts an HPUB frame's total byte count is header bytes plus payload length, producing exactly "HPUB subj 17 21" for a 17-byte header block and a 4-byte payload.
- `testParseServerInfoAcceptsNestingAtDepthBoundary` - Asserts parseServerInfo() decodes INFO JSON nested 511 levels deep, pinning the decoder's depth limit at exactly 512.
- `testParseServerInfoRejectsNestingBeyondDepthBoundary` - Asserts parseServerInfo() throws JsonException for INFO JSON nested 512 levels deep, pinning the upper edge of that same depth limit.

### tests/Unit/Mutation/ProtocolParserMutationTest.php
- `testUnterminatedControlLineAtExactBoundaryWithOffsetIsBuffered` - Asserts an unterminated control line whose remaining length is exactly 1 MiB after a consumed PING is buffered rather than rejected, pinning the OOM guard's exclusive boundary and its remaining-length subtraction.
- `testUnterminatedControlLineBelowBoundaryWithOffsetIsBufferedNotThrown` - Asserts an unterminated tail below the 1 MiB cap is buffered after a consumed PING, isolating the remaining-length subtraction from the boundary comparison.
- `testInfoPayloadKeepsFirstByteAfterVerb` - Asserts the INFO payload is taken from offset 4 so the leading "{" of the JSON body survives.
- `testErrPayloadKeepsFirstByteAfterVerb` - Asserts the -ERR payload is taken from offset 4 so the leading quote of the error text survives.
- `testMalformedMsgLineMessageIsPrefixThenLine` - Asserts a malformed MSG control line throws ProtocolException reading "Invalid MSG frame line: " followed by the offending line.
- `testMsgOversizeMessageIsPrefixThenSize` - Asserts an over-cap MSG payload throws ProtocolException reading "MSG frame payload size is invalid: " followed by the declared size.
- `testMalformedHmsgLineMessageIsPrefixThenLine` - Asserts a malformed HMSG control line throws ProtocolException reading "Invalid HMSG frame line: " followed by the offending line.
- `testHmsgTotalBytesAtExactMaxFrameSizeIsAccepted` - Asserts an HMSG whose total bytes equal maxFrameSize exactly is accepted and parsed, pinning the inclusive upper bound of the size check.
- `testHmsgOversizeMessageIsPrefixThenTotalBytes` - Asserts an over-cap HMSG throws ProtocolException reading "HMSG frame payload size is invalid: " followed by the declared total bytes.
- `testHmsgHeaderBytesExceedTotalMessageIsPrefixThenLine` - Asserts an HMSG declaring more header bytes than total bytes throws ProtocolException reading "HMSG header bytes exceed total bytes: " followed by the offending line.
- `testOutOfRangeSizeActuallyThrows` - Asserts an out-of-range 20-digit size in a frame line actually throws ProtocolException ("out of range in frame line") instead of being int-coerced and accepted.
- `testMsgMissingTerminatorErrorIsLabelledMsg` - Asserts an MSG payload without its trailing CRLF fails with exactly "MSG frame payload must be terminated by CRLF", labelled for the MSG branch.
- `testHmsgMissingTerminatorErrorIsLabelledHmsg` - Asserts an HMSG payload without its trailing CRLF fails with exactly "HMSG frame payload must be terminated by CRLF", labelled for the HMSG branch.

### tests/Unit/Mutation/PubAckMutationTest.php
- `testMissingSeqDefaultsToExactlyZero` - Asserts `PubAck::fromArray()` with no 'seq' key assigns a sequence of exactly 0, so the default is shifted to neither -1 nor 1.
- `testMissingDuplicateDefaultsToFalse` - Asserts a missing 'duplicate' key leaves the flag exactly false, so an ack is never reported as a duplicate by default.
- `testBatchCountIsCastToInt` - Asserts a 'count' supplied as the string "5" is coerced by the `(int)` cast, so batchCount is the integer 5 and not the raw string.
- `testBatchIdIsCastToString` - Asserts a 'batch' supplied as the int 42 is coerced by the `(string)` cast, so batchId is the string "42" and not the raw int.

### tests/Unit/Mutation/PullConsumerIteratorMutationTest.php
- `testFiniteIterationsRunsLoopExactlyOnce` - Asserts iterations=1 issues exactly one pull request and processes its single message, never attempting a second pull (#120 pipelined pull engine).
- `testFiniteIterationsRunsLoopExactlyTwice` - Asserts iterations=2 issues exactly two pull requests and delivers both messages in order, then stops without a third pull.
- `testSetNoWaitDefaultEmitsNoWaitTrueInPull` - Asserts `setNoWait()` called with no argument defaults to true, so the issued pull request carries `"no_wait":true`.
- `testInfiniteModeContinuesPast408Timeout` - Asserts a 408 request timeout is a routine empty window in infinite mode, so the loop keeps polling and delivers the message that arrives on the next pull.
- `testInfiniteModeStopsOnTerminal409` - Asserts a terminal 409 "Consumer Deleted" stops the infinite loop at once, with no further pull issued and no message delivered.
- `testPinNotCapturedWhenNoGroupConfigured` - Asserts no pin id is captured when no group is configured, so a delivered Nats-Pin-Id never leaks into a later pull request as an "id" field.
- `testCapturedPinIdIsRetainedAcrossIterations` - Asserts a pin id captured for a group is retained across iterations, so a later message advertising a different pin never overwrites the id sent on subsequent pulls.

### tests/Unit/Mutation/PullConsumerIterator_1MutationTest.php
- `testIdleBackoffFloorGuardsNegativeShiftAtZero` - Asserts `idleBackoffMs(0)` clamps the shift exponent at zero and returns the 10ms initial delay instead of shifting by a negative exponent (#153).
- `testIdleBackoffHoldsInitialDelayForFirstEmptyPull` - Asserts the first consecutive empty pull still pauses for the 10ms initial delay, so the backoff has not escalated yet (#153).
- `testIdleBackoffClampsAtCeiling` - Asserts the curve's last un-clamped step is 320ms at six consecutive empty pulls and that every pull beyond it clamps to the 500ms ceiling (#153).

### tests/Unit/Mutation/ScheduleMutationTest.php
- `testCronTrimsSurroundingWhitespace` - Asserts `cron()` trims surrounding whitespace before validating, returning the bare six-field expression rather than the padded input verbatim.
- `testPredefinedTrimsAliasBeforeNormalizing` - Asserts `predefined()` trims the alias before lowercasing and normalizing, so a padded " daily " resolves to "@daily" instead of failing validation.
- `testPredefinedRejectsAliasWithEmbeddedAtPrefix` - Asserts `predefined('x@daily')` throws InvalidArgumentException, so a known alias must match from the very start of the normalized string and junk before an embedded "@alias" cannot pass.
- `testPredefinedRejectsAliasWithTrailingJunk` - Asserts `predefined('dailyx')` throws InvalidArgumentException, so trailing junk after a known alias cannot slip through as a prefix match.
- `testUnknownAliasExceptionMessageIsExact` - Asserts an unknown alias throws with exactly the message `Unknown schedule alias "fortnightly": expected hourly, daily, weekly, monthly, yearly, annually, or midnight`, pinning every concatenation operand and its order.

### tests/Unit/Mutation/ServerInfoMutationTest.php
- `testBooleanConstructorDefaultsAreFalse` - Asserts the tlsRequired, tlsAvailable and lameDuckMode constructor defaults are all false when callers pass only the required positional arguments.
- `testConnectUrlsKeepsOnlyNonEmptyStrings` - Asserts connect_urls filtering keeps an entry only when it is a string AND non-empty, dropping both the empty string and the non-string entry rather than keeping either.
- `testJetStreamDefaultsToFalseWhenMissing` - Asserts jetStreamEnabled defaults to false when the INFO payload omits the "jetstream" key.
- `testMaxPayloadDefaultsToZeroWhenMissing` - Asserts maxPayload defaults to exactly 0 when the INFO payload omits "max_payload".
- `testHeadersSupportedDefaultsFalseAndHonoursPresentTrue` - Asserts headersSupported is false when "headers" is missing and true when the payload carries it, so the value comes from the payload rather than a constant.
- `testNonceIsCastToString` - Asserts a non-string nonce (int 123) is coerced by the `(string)` cast to "123", honouring the typed property contract.
- `testTlsAvailableDefaultsFalseAndHonoursPresentTrue` - Asserts tlsAvailable is false when "tls_available" is missing and true when the payload carries it.

### tests/Unit/Mutation/ServiceEndpointMutationTest.php
- `testProcessingTimeNsDefaultsToExactlyZero` - Asserts an endpoint constructed without a processing time starts its accumulator at exactly 0, neither 1 nor -1.
- `testProcessingTimeNsHonoursExplicitValueAndOtherCountersDefaultToZero` - Asserts an explicitly supplied processingTimeNs is stored verbatim while the requests and errors counters stay at their own zero defaults, proving the field reads from its constructor argument rather than a constant.

### tests/Unit/Mutation/ServiceErrorMutationTest.php
- `testParentConstructorPropagatesDescriptionAsExceptionMessage` - Asserts ServiceError forwards its description to the parent RuntimeException constructor so getMessage() returns 'Rate limited', alongside the independently set description, serviceErrorCode and body properties.
- `testMessageIsNonEmptyWhenThrownAndCaught` - Asserts a thrown and caught ServiceError still exposes its description on the Throwable message channel, so the message is never left as the empty default.

### tests/Unit/Mutation/ServiceGroupMutationTest.php
- `testGroupPrefixIsTrimmedOfSurroundingDots` - Asserts a group prefix of '.svc.' is stripped of its surrounding dots before being dot-joined to a clean endpoint subject, so the registered subject is 'svc.echo' rather than '.svc..echo'.
- `testEndpointSubjectIsTrimmedOfSurroundingDots` - Asserts an endpoint subject of '.echo.' is stripped of its surrounding dots before being dot-joined to a clean group prefix, so the registered subject is 'svc.echo' rather than 'svc..echo.'.

### tests/Unit/Mutation/Service_1MutationTest.php
- `testVersionRegexRequiresLeadingAnchor` - Asserts the service version check is anchored at the start of the string, so 'x1.2.3' is rejected with an InvalidArgumentException naming 'semantic version' even though it carries a valid semver suffix.
- `testVersionRegexRequiresTrailingAnchor` - Asserts the service version check must match to the end of the string, so '1.2.3 ' with trailing junk is rejected as not a semantic version.
- `testServiceIdIsSixteenHexChars` - Asserts the generated service id is exactly 16 lowercase hex characters, the hex encoding of 8 random bytes.
- `testWhitespaceOnlySubjectIsRejected` - Asserts an endpoint subject of only whitespace is trimmed before the emptiness guard and rejected with 'subject must not be empty'.
- `testValidationGateRequiresBothSchemaAndValidator` - Asserts request validation runs only when a schema and a validator are both present, so an endpoint with a schema but no validator dispatches to the handler, replies normally and records no errors.
- `testValidationProcessingTimeIsAccumulatedDifferenceNotSum` - Asserts the validation-rejection path records each request duration as an elapsed difference, keeping the accumulated processing time a small non-negative value rather than an absolute clock reading.
- `testValidationProcessingTimeAccumulatesAcrossRequests` - Asserts the endpoint's processing time on the validation-rejection path is the running sum of the per-request duration_ns values reported to observers across two requests.
- `testValidationRequestErrorContextCarriesValidationErrorCode` - Asserts the request_error observer context for a schema rejection carries code 'VALIDATION_ERROR' together with the validator's message.
- `testValidationRequestEndContextCarriesDurationKey` - Asserts the request_end observer context on the validation-rejection path includes the duration_ns key.
- `testValidationErrorReplyCarriesCorrelationIdFromHeader` - Asserts a validation error reply embeds the request's X-Request-Id header value as correlation_id in the published JSON payload.
- `testServiceErrorEmitsRequestErrorEvent` - Asserts a handler that throws a ServiceError emits a request_error observer event.
- `testServiceErrorRequestErrorContextCarriesCodeAndError` - Asserts the request_error context for a thrown ServiceError carries both the service error code ('429') and its description ('Rate limited').
- `testHandlerErrorEmitsRequestErrorEvent` - Asserts an uncaught handler exception emits a request_error observer event.
- `testHandlerErrorRequestErrorContextCarriesCodeAndMessage` - Asserts the request_error context for an uncaught handler exception carries code 'HANDLER_ERROR' plus the raw exception message on the server side.
- `testHandlerPathProcessingTimeIsDifferenceNotSum` - Asserts the handler path records its duration as an elapsed difference, keeping the accumulated processing time a small non-negative value.
- `testHandlerPathProcessingTimeAccumulates` - Asserts the endpoint's processing time on the handler-exception path is the sum of the per-request duration_ns values observed across two requests.
- `testHandlerPathRequestEndContextCarriesDurationKey` - Asserts the request_end observer context on the handler path includes the duration_ns key.
- `testEncodeFailureIncrementsErrorCounter` - Asserts a handler returning un-encodable non-UTF-8 data increments the endpoint error counter to 1 (#97).
- `testEncodeFailureEmitsRequestErrorEvent` - Asserts the JSON-encode-failure path emits a request_error observer event (#97).
- `testEncodeFailureRequestErrorContextCarriesCodeAndError` - Asserts the encode-failure request_error context carries code 'HANDLER_ERROR' and a non-empty error string (#97).
- `testEncodeFailureReplyCarriesCorrelationId` - Asserts the controlled HANDLER_ERROR reply on the encode-failure path embeds the correlation id taken from the request header (#97).
- `testStartRollbackUnsubscribesAlreadySubscribedSids` - Asserts a start() that fails partway on an invalid endpoint subject rolls back by writing UNSUB frames for the subscriptions already registered.
- `testDrainFlushesWithExtraPing` - Asserts drain() flushes the connection, writing exactly one PING beyond those already sent.

### tests/Unit/Mutation/Service_2MutationTest.php
- `testDiscoverySubjectsAreSubscribedVerbatim` - Asserts start() subscribes verbatim to every discovery subject, both the name-only and name-plus-id forms of $SRV.PING, $SRV.INFO, $SRV.STATS and $SRV.SCHEMA.
- `testStatsEndpointEntryCarriesNameAndQueueGroup` - Asserts each statsSnapshot() endpoint entry carries the endpoint name, queue_group and subject under their literal keys.
- `testStatsResponseTopLevelKeysArePresent` - Asserts statsSnapshot() carries the top-level name, id, version, a non-empty started timestamp and the service metadata.
- `testPingResponseBaseCarriesNameIdVersionMetadata` - Asserts a $SRV.PING request is answered with an io.nats.micro.v1.ping_response carrying the service name, id, version and metadata.
- `testSchemaResponseEndpointEntryCarriesNameAndSubject` - Asserts a $SRV.SCHEMA request is answered with an io.nats.micro.v1.schema_response whose endpoint entry carries name and subject.
- `testInfoResponseEndpointEntryCarriesName` - Asserts a $SRV.INFO request is answered with an io.nats.micro.v1.info_response whose endpoint entry carries the endpoint name alongside its subject.
- `testObserverContextCarriesSubjectAndReplyTo` - Asserts the request_start observer context carries the request subject and the reply_to inbox.
- `testCorrelationIdPrefersRequestIdOverTraceparent` - Asserts x-request-id takes precedence over traceparent when both headers are present, so the observer context correlation_id is the request id.
- `testCorrelationIdPrefersTraceparentOverNatsMsgId` - Asserts traceparent takes precedence over nats-msg-id when no x-request-id is present.
- `testErrorPayloadOmitsCorrelationIdWhenAbsent` - Asserts a header-less request's error reply omits the correlation_id key entirely instead of emitting a null one.
- `testErrorReplyIsPublishedExactlyOnceWithHeaders` - Asserts a ServiceError reply is published exactly once as an HPUB carrying the error headers, with no second plain PUB to the same inbox.
- `testErrorHeaderCollapsesInternalWhitespace` - Asserts the Nats-Service-Error header value is the collapsed and trimmed description ('Rate limited'), never the raw multi-space form.
- `testRunWithoutCancellationDoesNotBreakBeforeProcessing` - Asserts run() with no timeout or cancellation does not break on its first iteration, serving the buffered request and publishing its reply before the peer close ends the loop.

### tests/Unit/Mutation/Service_3MutationTest.php
- `testInfoEndpointEntryUsesNamedKeys` - Asserts the info_response endpoint entry carries name, subject and queue_group under their literal string keys, with no stray integer-indexed element appended in their place.
- `testInfoResponseCarriesDescriptionKey` - Asserts the top-level info_response carries the service description under the literal 'description' key.
- `testClassHandlerInstantiationFailureUsesZeroCode` - Asserts a class-string handler that cannot be auto-instantiated is rejected with an InvalidArgumentException reading 'could not be instantiated', code 0, and the original error chained as its previous exception.

### tests/Unit/Mutation/Service_4MutationTest.php
- `testFinallyStopsServiceWhenLoopConditionThrows` - Asserts an error escaping the run loop still tears the service down through the finally, unsubscribing the discovery and endpoint subscriptions and clearing the started flag and sid list before the error propagates out of run().
- `testCancelledReadBreaksLoopImmediatelyWithoutServingMore` - Asserts a CancelledException from a run-loop read breaks the loop at once, so exactly one run-loop read happens and the request queued behind it is never read or answered.

### tests/Unit/Mutation/StreamConfigurationMutationTest.php
- `testSubjectsReindexesSplattedAssociativeKeysToAList` - Asserts subjects() reindexes its variadic arguments so the emitted config always carries a list-shaped subjects array with integer keys 0 and 1, even when an associative array is splatted into the call.
- `testSealedDefaultsToTrueWhenCalledWithoutArgument` - Asserts sealed() called with no argument records sealed:true in the config payload, pinning the parameter default at true rather than false.

### tests/Unit/Mutation/StreamInfoMutationTest.php
- `testNonStringSubjectsAreFilteredOut` - Asserts fromArray() keeps only the string entries of a stream config's subjects array, dropping ints, bools, nulls and nested arrays.
- `testSurvivingSubjectsAreReindexedToAList` - Asserts the subjects that survive filtering are reindexed to a contiguous 0-based list, so a non-string entry removed from the middle leaves no gap in the keys.

### tests/Unit/Mutation/SubscriptionQueueMutationTest.php
- `testDefaultMaxPendingIsExactly1024` - Asserts the default maxPending is exactly 1024, so under DropOldest a 1025th enqueue leaves the buffer holding 1024 messages headed by the second one enqueued.
- `testMaxOfOneFloorThrowsOnSecondEnqueueWithErrorPolicy` - Asserts a maxPending of 1 under the Error policy accepts the first enqueue and overflows with a NatsException on the second.
- `testMaxFloorClampsZeroToOneAllowingFirstEnqueue` - Asserts a maxPending of 0 is clamped up to a floor of 1, so the first enqueue succeeds and is retrievable instead of overflowing immediately.
- `testFetchReturnsBufferedMessageWithoutReading` - Asserts fetch() returns an already-buffered message through its early return without starting any socket read, proven by a blocking transport that records zero reads.
- `testNextReturnsBufferedMessageWithoutReading` - Asserts next() returns an already-buffered message through its early return without starting any socket read, so a set timeout never sends it into the wait path.
- `testTimeoutZeroNonBlockingBranchSurfacesMessage` - Asserts a timeout of exactly 0 takes the non-blocking single-cycle branch of next() and still surfaces the queued message rather than cancelling instantly.
- `testNextLoopsUntilMessageArrivesOnLaterCycle` - Asserts next() with a positive timeout keeps looping past an empty first read cycle and returns the message that arrives on a later cycle.
- `testNextBreaksOnFirstBufferedMessageLeavingRestBuffered` - Asserts next() breaks as soon as one message is buffered, returning the first delivery and leaving the second retrievable by a subsequent call.
- `testNextBreakStopsLoopWithoutFurtherReads` - Asserts next() breaks out of the wait loop the moment a message is buffered and never pumps the socket again, so an otherwise idle blocking transport records zero reads.
- `testFetchAllBufferedDrainRespectsLimitTwoOfThree` - Asserts fetchAll()'s buffered drain stops exactly at the requested limit, returning the first two of three pre-buffered messages in order.
- `testFetchAllNullLimitDrainsEntireBuffer` - Asserts fetchAll() with a null limit drains the whole buffer, returning all three pre-buffered messages in order.
- `testFetchAllEarlyReturnsExactlyAtLimitWithoutReading` - Asserts fetchAll() returns immediately when the buffered drain already meets the limit exactly, delivering both messages without starting any read on the blocking transport.
- `testFetchAllTimeoutZeroIsTreatedAsNoTimeoutSingleCycle` - Asserts a timeout of 0 counts as no timeout in fetchAll(), so an empty queue does a single best-effort cycle and returns an empty array instead of waiting.
- `testFetchAllInnerDrainStopsAtLimitWithinOneCycle` - Asserts the drain inside fetchAll()'s read loop stops exactly at the limit even when one read cycle buffers three frames at once, returning only the first two.
- `testFetchAllFinalDrainRespectsLimit` - Asserts fetchAll()'s final drain after the read window closes still caps at the limit, returning the first two of three messages enqueued while it was waiting.

### tests/Unit/Mutation/SubscriptionQueue_1MutationTest.php
- `testNextAssemblesChunkedFrameWithoutIdleSleepPerPartialChunk` - Asserts next() pays no idle yield on reads that consumed bytes, so a frame arriving as 317 one-byte partial chunks assembles and returns its 300-byte payload well inside a 60ms window (#119).
- `testNextIdleWaitYieldsPerEmptyReadInsteadOfBusySpinning` - Asserts next() yields once per genuinely empty read instead of busy-spinning, so a message sitting behind 150 idle reads is not reached within a 50ms window and the call honours its deadline by returning null (#119).

### tests/Unit/Mutation/SubscriptionQueue_2MutationTest.php
- `testFetchAllTimeoutZeroBreaksOnFirstEmptyFrameNotFullDrain` - Asserts a timeout of 0 makes fetchAll() a single best-effort cycle that breaks on the first read completing no frame, so a message split across two chunks never assembles and an empty array is returned.

### tests/Unit/Mutation/UnsupportedFeatureExceptionMutationTest.php
- `testDefaultExceptionCodeIsZeroWhenCodeArgOmitted` - Asserts an UnsupportedFeatureException built without a code argument reports code 0, with the feature, required version, server version and message preserved verbatim.
- `testExplicitNonDefaultCodeStillFlowsThrough` - Asserts an explicitly passed code flows through to getCode(), so the default-code guarantee is a real boundary at 0 rather than a hard-coded return.

### tests/Unit/Mutation/WebSocketFrameCodecMutationTest.php
- `testEncodeDefaultsToMaskedClientFrame` - Asserts encode() masks by default when the flag is omitted, setting the 0x80 mask bit and emitting the 4-byte mask key (8 bytes total for a 2-byte payload), pinning the masking default against a flip to unmasked.
- `testLength125UsesSingleByteLength` - Asserts a 125-byte payload stores its length directly in the 7-bit length field with a 2-byte header and no extended length field, pinning the inclusive upper bound of the 7-bit length form.
- `testLength65535UsesSixteenBitLength` - Asserts a 65535-byte payload uses the 126 marker with a 2-byte extended length (4-byte header total) rather than the 64-bit form, pinning the inclusive upper bound of the 16-bit length form.
- `testFinAndRsv1ReadOnlyTheirOwnBits` - Asserts decode() reads FIN strictly from bit 0x80 and RSV1 strictly from bit 0x40, so a non-final text frame (byte1=0x01) reports fin=false and rsv1=false with the opcode low bit never leaking into either flag.
- `testSixteenBitHeaderAcceptedAtExactFourBytes` - Asserts decode() emits a 16-bit-marker frame declaring length 0 as soon as its 4 header bytes are buffered and fully consumes the buffer, pinning the header availability boundary against off-by-one mutations that would withhold the frame.
- `testSixtyFourBitHeaderWaitsForFullEightLengthBytes` - Asserts decode() returns no frames and leaves the buffer untouched when a 127-marker header carries only 7 of its 8 length bytes, so a 64-bit length is never unpacked from a short prefix.
- `testMaxFramePayloadBoundaryIsAccepted` - Asserts a frame declaring exactly MAX_FRAME_PAYLOAD is within bounds and simply waits for its payload, returning no frames, an untouched buffer and a null terminal violation, pinning the inclusive upper bound.
- `testFragmentedCloseFrameIsATerminalControlViolation` - Asserts a Close frame (opcode 0x8, the lowest control opcode) with FIN cleared is reported as a terminal ProtocolException mentioning "control frame" and yields no decoded frames, so RFC 6455 5.5 covers every control opcode including Close.
- `testControlFrameWithExactly125BytePayloadIsAccepted` - Asserts a ping carrying exactly 125 payload bytes decodes normally with no terminal violation, pinning the inclusive 125-byte control-frame payload cap.
- `testOutOfBoundsLengthReportsExactTerminalMessage` - Asserts a declared length of MAX_FRAME_PAYLOAD+1 produces a terminal ProtocolException whose message is exactly "WebSocket frame payload length out of bounds: <length>", pinning both the prefix order and the inclusion of the offending length.
- `testMaskKeyAcceptedAtExactSixBytes` - Asserts decode() with allowMasked emits a masked zero-length frame as soon as header plus 4-byte mask key (6 bytes) are buffered and fully consumes the buffer, pinning the mask-key availability boundary.
- `testDeflateStripsExactlyFourByteEmptyBlock` - Asserts deflate() removes exactly the trailing 4-byte empty DEFLATE block, producing output that no longer ends with 0x00 0x00 0xff 0xff, is exactly 4 bytes shorter than the raw sync-flushed stream, equals that stream minus its last 4 bytes, and still round-trips through inflate().

### tests/Unit/Mutation/WebSocketTransport_1MutationTest.php
- `testConnectClampsTimeoutToOneMillisecondFloor` - Asserts connect() clamps the recorded connect timeout to a 1ms floor, so a requested timeout of 0 is stored as 1 even when the socket attempt itself fails.
- `testConnectResetsFragmentingAndCompressionState` - Asserts connect() clears the fragment-reassembly and compression-active flags at the start of every attempt, before the invalid-DSN failure is raised.
- `testConnectRejectsParseableDsnWithoutHost` - Asserts a DSN that parses but carries no host is rejected with ConnectionException instead of proceeding host-less.
- `testConnectInvalidDsnMessageIncludesDsnInOrder` - Asserts the invalid-DSN exception message is exactly "Invalid WebSocket DSN: " followed by the offending DSN.
- `testConnectTreatsUppercaseWssAsSecure` - Asserts an upper-case WSS:// scheme is still treated as secure, so TLS is negotiated and a plain-TCP server surfaces a TlsException.
- `testHandshakePreservesNonRootPath` - Asserts the upgrade handshake emits the DSN path verbatim in its GET request line rather than collapsing a non-root path to "/".
- `testHandshakeAppendsQueryAfterPathWithQuestionMark` - Asserts the upgrade handshake appends the query string after the path separated by "?" in that order, producing "GET /foo?q=v HTTP/1.1".
- `testReadLineAccumulatesPartialFrameAcrossReads` - Asserts bytes from successive socket reads are appended to the read buffer, so a frame split across two reads is eventually decoded and returned by readLine().
- `testCloseSendsCloseFrameToPeer` - Asserts close() writes a best-effort CLOSE control frame to the peer when a socket is still connected.
- `testCloseClosesUnderlyingSocket` - Asserts close() closes the underlying socket rather than leaving it open.
- `testDrainDecodesTextFrameAsData` - Asserts an OP_TEXT frame is decoded as application data alongside OP_BINARY, so its payload is returned.
- `testDrainPingWithoutSocketDoesNotError` - Asserts a PING received with no socket set is answered through a null-safe write, so draining completes with an empty result instead of erroring.
- `testDrainConcatenatesMultipleCompleteFrames` - Asserts two complete data frames present in one buffer are concatenated into the returned payload rather than only the last one surviving.
- `testDrainRejectsOversizedFirstFragment` - Asserts the fragment size bound is enforced on the opening non-final fragment too, so a first fragment already over the cap throws ProtocolException.
- `testDrainConcatenatesCompletedFragmentOntoPriorData` - Asserts a completed fragmented message is appended to data already decoded in the same drain pass, so the earlier complete frame is not dropped.
- `testDrainResetsFragmentStateAfterCompletion` - Asserts the fragmenting and fragment-compressed flags are cleared once a fragmented message completes, so the next message starts from clean state.
- `testFragmentExactlyAtCapIsAccepted` - Asserts the reassembly bound is inclusive, so a message of exactly maxMessageBytes is accepted rather than rejected.
- `testEnforceResetsStateBeforeThrowing` - Asserts the fragmenting and fragment-compressed flags are reset before the over-cap ProtocolException is thrown, so a subsequent message is not corrupted.

### tests/Unit/Mutation/WebSocketTransport_2MutationTest.php
- `testCompressionDefaultsToOffWhenArgumentOmitted` - Asserts buildUpgradeRequest() defaults compression to off, so omitting the argument emits neither a permessage-deflate offer nor a Sec-WebSocket-Extensions header.
- `testCompressionOfferEmittedWhenExplicitlyEnabled` - Asserts passing the compression flag explicitly still emits the "Sec-WebSocket-Extensions: permessage-deflate" offer, so the default-off pin cannot be satisfied by disabling the feature outright.
- `testNonEmptyTlsPeerNameIsHonoredOverHost` - Asserts a non-empty tlsPeerName is used verbatim as the TLS context peer name instead of being overwritten by the connection host.
- `testEmptyTlsPeerNameFallsBackToHost` - Asserts an empty-string tlsPeerName falls back to the host, pinning the empty-string arm of the peer-name guard.
- `testNullTlsPeerNameFallsBackToHost` - Asserts a null tlsPeerName falls back to the host, pinning the null arm of the peer-name guard.
- `testPeerVerificationFollowsOption` - Asserts peer verification is disabled exactly when tlsVerifyPeer is false and stays enabled when it is true, so the guard cannot be inverted.
- `testNonEmptyCaFileIsApplied` - Asserts a non-empty tlsCaFile is applied to the TLS context as its CA file.
- `testEmptyCaFileIsNotApplied` - Asserts an empty-string tlsCaFile is rejected by the guard and leaves the context without a CA file rather than installing an empty path.
- `testNullCaFileLeavesContextUnchanged` - Asserts a null tlsCaFile leaves the TLS context with no CA file configured.
- `testNonEmptyCertFileInstallsCertificate` - Asserts a non-empty tlsCertFile installs a client certificate for mTLS carrying exactly the configured cert and key paths.
- `testNullCertFileInstallsNoCertificate` - Asserts a null tlsCertFile installs no client certificate, so mTLS stays off by default.

### tests/Unit/Mutation/WebSocketTransport_3MutationTest.php
- `testConnectAppliesReadChunkSizeToSocket` - Asserts connect() applies the configured read chunk size to the freshly connected socket before the handshake, so the socket's reader chunk size is the configured 4096 rather than Amp's 8192 default even when the handshake then fails (#119).
- `testApplyReadChunkSizeSetsConfiguredSizeOnResourceSocket` - Asserts applyReadChunkSize() calls setChunkSize on a real ResourceSocket when the configured size is positive, so the reader chunk size becomes exactly the configured value (#119).
- `testApplyReadChunkSizeLeavesSocketUntouchedForZeroChunkSize` - Asserts a configured chunk size of 0 fails the strictly-positive guard, so setChunkSize is never called, nothing throws and the reader keeps Amp's 8192 default (#119).

### tests/Unit/Mutation/WebSocketTransport_4MutationTest.php
- `testAppliesConfiguredReadChunkSizeToResourceSocket` - Asserts applyReadChunkSize() really caps reads on a ResourceSocket, so with a 16-byte chunk size a single read of 256 buffered bytes returns exactly 16 (#119).
- `testDoesNotCallSetChunkSizeOnNonResourceSocket` - Asserts applyReadChunkSize() is a side-effect-free no-op when the socket is not a ResourceSocket, so the missing setChunkSize is never called and the socket still hands out its scripted chunk unchanged (#119).

### tests/Unit/Mutation/WebSocketTransport_5MutationTest.php
- `testConsumedSpilledFrameLeavesNoStaleBufferRemainder` - Asserts that once a spilled 64-bit frame whose payload ends exactly on a chunk boundary is consumed, readLine() returns the full 65536-byte payload and the working buffer is left empty with the spill sizing retired, so neither header nor already consumed payload bytes are re-injected (#164).
- `testLargeSixteenBitFrameIsNeverSizedForSpill` - Asserts an incomplete 16-bit-length frame (60000 bytes, 126 marker) with a large outstanding tail is never sized for chunk-list spill, since only frames carrying the 64-bit length marker qualify (#164).
- `testNearlyCompleteLargeFrameIsNotSpilled` - Asserts a 64-bit-length frame whose outstanding tail is only 6 bytes stays on the batch-decode append path rather than spilling, pinning both the tail arithmetic and the conjunction in the spill gate (#164).
- `testFrameWithTailExactlyAtThresholdIsNotSpilled` - Asserts the spill gate is a strict greater-than, so a 64-bit frame whose outstanding tail equals the 32768-byte threshold exactly is not sized for spill (#164).
- `testCloseEchoMirrorsOnlyTheTwoStatusBytes` - Asserts the Close echo mirrors only the two status bytes of a peer Close that also carries a trailing reason phrase, while the close itself still surfaces as TransportClosedException (#161).
- `testDataFrameMidFragmentationResetsFragmentState` - Asserts a new data frame arriving mid-fragmentation (RFC 6455 5.4) resets the in-progress fragment state before the violation is flagged, so no abandoned partial message survives the ProtocolException (#115).
- `testFragmentBoundCountsAllContinuationFrames` - Asserts the fragment bound is enforced on the cumulative reassembly size, so three 4-byte continuations on a 2-byte opener trip a 10-byte cap even though no single frame exceeds it (#89).
- `testHandshakeReadsUntilHeaderTerminator` - Asserts connect() keeps reading the upgrade response until the CRLFCRLF header terminator is seen and then validates the 101 status and accept key, completing without error against a loopback server that performs a correct RFC 6455 upgrade and leaving tlsActive() false for ws://.
- `testHeaderTopUpDecrementsChunkAccountingBeforeDeferringIncompleteHeader` - Asserts that when the header top-up loop exhausts the queued chunks without completing a sized frame's header, every folded chunk's length has been subtracted from the chunk accounting before the "header incomplete" ProtocolException is deferred, leaving no phantom byte count behind (#164).
- `testSpanningFrameFoldsOnlyChunksBeyondTheConsumedPayload` - Asserts the spanning consume re-injects only the chunks past the consumed payload, delivering the 70000-byte frame plus a trailing frame split across two further chunks byte-exactly and fully retiring the spill state (#164).
- `testCloseTerminalIsNotOverwrittenByTrailingCodecViolation` - Asserts that when one read carries a Close frame followed by a masked frame, the first terminal condition wins and the graceful TransportClosedException is surfaced instead of the later ProtocolException.
- `testDeferredCloseSurvivesTrailingOversizedLengthDeclaration` - Asserts a read carrying data, a Close, and an incomplete frame declaring an out-of-bounds 64-bit length returns the data first and then surfaces the deferred close, never replacing it with the oversize ProtocolException from the head-frame sizing pass.
- `testAnswerControlFrameWithoutSocketTouchesNeitherSlotsNorWriter` - Asserts queuing a pong or close echo while no socket is connected returns before filling either answer slot or arming the writer fiber, so no queued answer can chase the next connection's socket.
- `testFirstQueuedCloseEchoIsNeverReplacedByALaterClose` - Asserts at most one Close echo is ever written per connection and it carries the first Close's status, so a later Close cannot overwrite a queued but unsent echo (RFC 6455 5.5.1).
- `testWedgedAnswerWriterIsNeverDuplicatedByNewerAnswers` - Asserts a newer control answer only refills its slot while the single writer fiber is wedged on a backpressure-suspended write, never spawning a second writer that would issue concurrent writes on the same socket, and that the slot drains once the wedged write is released.
- `testWriterFiberRetiresSoLaterPingsAreStillAnswered` - Asserts the control-answer writer retires once both slots are empty, so pings delivered in two separate reads are each answered by a fresh writer with a pong carrying its own ping's payload.
- `testRsv1WithoutNegotiationRejectsEvenAValidDeflatePayload` - Asserts an RSV1 frame without a negotiated extension fails the connection with ProtocolException ("permessage-deflate was not negotiated") even when its payload is a well-formed DEFLATE stream, so inflated attacker-shaped bytes are never delivered (RFC 6455 5.2).
- `testFramesAfterACorruptCompressedFrameAreDropped` - Asserts a frame whose compressed payload fails to inflate is terminal and the data frames after it in the same read are dropped, so the drain throws the typed inflate error instead of delivering the trailing payload.
- `testFramesAfterAnOversizedFirstFragmentAreDropped` - Asserts an over-cap first fragment is terminal and the frames after it in the same read are dropped, so the drain throws the bound violation instead of delivering the trailing frame.
- `testFramesAfterAnOversizedContinuationAreDropped` - Asserts an over-cap continuation is terminal and the frames after it in the same read are dropped, so the drain throws the bound violation instead of delivering the trailing frame.
- `testCorruptCompressedFragmentDropsLaterFramesAndResetsState` - Asserts a fragmented compressed message whose reassembled stream fails to inflate surfaces the typed inflate error, drops the frames after it in the same read, and leaves the transport back at the not-fragmenting baseline with the fragment buffers and counters cleared.
- `testInflatedFrameAppendsToSameReadPlainData` - Asserts an inflated RSV1 frame's output is appended to data already decoded from the same read, so a plain frame followed by a compressed frame yields their concatenation in order.
- `testInflatedFragmentedMessageAppendsToSameReadPlainData` - Asserts a completed compressed fragmented message's output is appended to data already decoded from the same read, exactly like the unfragmented case.

### tests/Unit/MuxRequestInboxInternalsTest.php
- `testDispatchMuxReplyRoutesReplyToItsTokenWaiter` - A mux reply is delivered to the waiter registered for its token.
- `testDispatchMuxReplyDropsReplyForRemovedOrUnknownToken` - A reply for a token that is gone (timed out or already completed) is dropped instead of resurfacing on another request.
- `testDispatchMuxReplyRejectsForeignBaseSubject` - A message whose subject does not belong to this connection's mux base is not treated as a mux reply.
- `testNewMuxTokenProducesDistinctTokens` - Successive mux tokens are distinct, so two in-flight requests never share a reply address.
- `testUnboundedSidBypassesSlowConsumerDrop` - A subscription marked slow-consumer exempt keeps buffering past the pending cap instead of dropping messages (#118/#120).
- `testNonExemptSidStillDropsUnderSlowConsumerPolicy` - A normal subscription still applies the configured slow-consumer drop policy, so the exemption is scoped.
- `testReleaseRuntimeStateClearsMuxState` - Releasing runtime state clears the mux inbox bookkeeping so a reconnect starts clean.
- `testDropSubscriptionStateClearsUnboundedFlag` - Dropping a subscription clears its slow-consumer exemption, so a recycled sid does not inherit it.

### tests/Unit/MuxRequestInboxTest.php
- `testRequestRoutesReplyThroughMuxInbox` - A reply addressed to the shared mux inbox token reaches the request that issued it (#118).
- `testManyRequestsShareOneMuxSubscriptionWithNoUnsub` - Many requests reuse one mux subscription: only one SUB is written and no per-request UNSUB churn appears on the wire.
- `testDistinctRequestsGetDistinctReplyTokens` - Concurrent requests are addressed with distinct reply tokens so their replies cannot be confused.
- `testRequestRemovesItsWaiterAfterCompletion` - A completed request removes its waiter, so the mux waiter map does not grow with request volume.
- `testRequestManyRemovesItsWaiterAfterCompletion` - requestMany() likewise removes its collector once the batch completes.

### tests/Unit/NatsClientTest.php
- `testClientConnectAndPublishDelegatesToConnection` - asserts the `NatsClient` facade connects and publishes, exposing `serverInfo()` (serverName `n1`) and writing the expected `PUB orders.created` frame as the third transport write.
- `testClientSubscribeAndProcessIncoming` - asserts `subscribe` returns sid 1, `processIncoming` returns 1, and the registered callback receives the dispatched message with payload `hello`.
- `testClientRequestReturnsReply` - asserts `request` resolves with the first reply message (payload `hello`).
- `testClientRequestCanBeCancelled` - asserts `request` forwards a pre-cancelled cancellation and throws `CancelledException`.
- `testClientPublishWithHeadersAndRequestWithHeaders` - asserts `publishWithHeaders` and `requestWithHeaders` emit `HPUB` frames carrying the supplied headers (`X-Test:1`, `X-Correlation-Id:abc`) and that the request resolves with reply payload `{"ok":true}`.
- `testDiscoveredServersReturnsConnectUrls` - asserts `discoveredServers()` returns the `connect_urls` advertised in the server INFO frame.
- `testClientServiceFactoryDisconnectAndDrain` - asserts `service()` returns a `Service`, `subscribe` returns sid 1, `drain()` closes the transport, and a separate client's `disconnect()` also closes its transport.

### tests/Unit/NatsConnectionInternalsTest.php
- `testNormalizeDsnConvertsNatsScheme` - asserts private `normalizeDsn` rewrites `nats://` to `tcp://` while leaving a `tls://` DSN unchanged.
- `testNextServerRoundRobinAndFallback` - asserts private `nextServer` cycles configured servers round-robin and falls back to `nats://127.0.0.1:4222` when the server list is empty.
- `testValidateSubjectPrivateBranches` - asserts `validateSubject` throws `ProtocolException` "Wildcards must occupy an entire token" for `orders.a*`.
- `testValidateSubjectRejectsGreaterThanMiddleToken` - asserts `validateSubject` throws `ProtocolException` "Wildcard \">\" must be the last token" for `orders.>.created`.
- `testIsNoRespondersStatusPrivateChecks` - asserts `isNoRespondersStatus` returns false for no headers and for a 200 header, and true for a `503 No Responders` header.
- `testExtractHeadersAndPayloadPrivatePaths` - asserts `extractHeadersAndPayload` returns null headers/payload for a MSG frame, splits headers/body for a valid HMSG frame, and throws `ProtocolException` "Malformed HMSG frame" when `headerBytes` exceeds payload length.
- `testRecoverConnectionDisabledThrowsImmediately` - asserts `recoverConnection` throws `ConnectionException` "Reconnect is disabled" when reconnect is disabled.
- `testRecoverConnectionExhaustedSetsClosedState` - asserts that with all connect attempts failing, `recoverConnection` throws `ConnectionException` "Reconnect attempts exhausted" and leaves the connection in Closed state.
- `testConnectReturnsImmediatelyWhenAlreadyOpen` - asserts `connect()` is a no-op (no transport connect calls, stays Open) when the connection is already Open.
- `testAwaitServerInfoThrowsWhenInfoNeverArrives` - asserts `awaitServerInfo` throws `ConnectionException` "Expected INFO during connect" when no INFO line ever arrives.
- `testAwaitInitialPongThrowsWhenPongNeverArrives` - asserts `awaitInitialPong` throws `ConnectionException` "Expected PONG after CONNECT" when no PONG ever arrives.
- `testAwaitInitialPongHandlesParsedControlFrames` - asserts `awaitInitialPong` handles parsed control frames in a combined buffer and responds to PING with a `PONG\r\n` write.
- `testAwaitInitialPongThrowsOnParsedErrFrame` - asserts `awaitInitialPong` throws `ConnectionException` "Server error during connect" on a parsed `-ERR` frame.
- `testAwaitServerInfoAllowsMoreThanEightPollsBeforeInfoArrives` - asserts `awaitServerInfo` keeps polling past 8 empty reads and parses the INFO once it arrives (serverId `S9`).
- `testAwaitInitialPongAllowsMoreThanEightPollsBeforePongArrives` - asserts `awaitInitialPong` keeps polling past 8 `+OK` reads and returns the (here empty) list of frames coalesced behind the PONG once it arrives (#157).
- `testAwaitServerInfoRespondsToPingBeforeInfo` - asserts `awaitServerInfo` replies to a PING received before INFO with `PONG\r\n` and then parses the INFO (serverId `S4`).
- `testAwaitServerInfoParsesInfoLine` - asserts `awaitServerInfo` parses a raw INFO line into a `ServerInfo` with serverId `S1`.
- `testAwaitServerInfoParsesInfoFrame` - asserts `awaitServerInfo` parses an INFO frame into a `ServerInfo` with serverId `S2`.
- `testAwaitInitialPongThrowsOnErrLine` - asserts `awaitInitialPong` throws `ConnectionException` "Server error during connect" on a `-ERR Permissions Violation` line.
- `testHandleFramePongResetsOutstandingPingAndCompletesOldestPongSlot` - asserts a Pong frame in `handleFrame` resets `outstandingPings` to 0 and completes only the OLDEST queued pong slot (FIFO PING/PONG correlation, #117), leaving later slots waiting.
- `testHandleFrameErrThrowsConnectionException` - asserts an Err frame in `handleFrame` throws `ConnectionException` "Server sent error frame".
- `testHandleFrameInfoUpdatesServerInfo` - asserts an Info frame in `handleFrame` updates `serverInfo()` (serverId `S2`, maxPayload 2048).
- `testHandleFrameRecoverableErrDoesNotThrow` - asserts a recoverable "Permissions Violation for Publish" Err frame does not throw and leaves the state Open.
- `testHandleFrameIgnoresUnknownSubscriptionSid` - asserts a Msg frame for an unknown sid is ignored and no pending messages are buffered.
- `testDrainAllPendingDeliversBufferedMessagesInOrder` - asserts `drainAllPending` delivers buffered messages to the subscription callback in FIFO order (`['a','b']`).
- `testEnforceMaxPayloadAllowsUnknownServerInfoAndThrowsWhenExceeded` - asserts `enforceMaxPayload` is a no-op without server info, then throws `ProtocolException` "exceeds server max_payload" once a max_payload-8 ServerInfo is set and 9 bytes are checked.
- `testDrainPendingForSidNoOpWhenStateMissing` - asserts `drainPendingForSid` returns null (no-op) when no pending state exists for the sid.
- `testIsNoRespondersStatusHandlesEmptyRawHeaderString` - asserts `isNoRespondersStatus` returns false for a message with an empty raw header string.
- `testStartPingTimerCancelsWhenConnectionStateIsNotOpen` - asserts `startPingTimer` cancels itself (clears `pingTimerId`) when the connection is not Open.
- `testStartPingTimerWriteFailureClosesWhenReconnectDisabled` - asserts a PING write failure with reconnect disabled drives the connection to Closed and clears `pingTimerId`.
- `testDropSubscriptionStateRemovesEntries` - asserts `dropSubscriptionState` removes the subscription, meta, and pending-message entries for the sid.

### tests/Unit/NatsConnectionTest.php

- `testConnectTransitionsToOpenAndSendsConnectAndPing` - Successful handshake moves state to Open, populates serverInfo, dials the configured DSN, and writes exactly CONNECT then PING.
- `testConnectHandlesOkAndPingBeforePong` - Handshake tolerates a +OK line and an interleaved server PING, replying PONG before the final handshake PONG.
- `testConnectReassemblesFragmentedInfoFrame` - A long INFO (with xkey) split across two TCP segments is buffered and reassembled so the handshake succeeds (regression #2).
- `testConnectFailsOnUnknownControlLineDuringHandshake` - An unknown control op during handshake raises ConnectionException ("Unsupported control frame: UNKNOWN") and leaves state Closed.
- `testConnectFailsWhenNoPongAndMovesToClosed` - A partial line without CRLF exhausts the poll budget and times out with "Expected PONG after CONNECT", leaving state Closed.
- `testConnectFailsOnServerErrLine` - A server -ERR line during handshake fails fast with ConnectionException ("Server error during connect").
- `testConnectAuthErrorThrowsAuthenticationExceptionWithoutRetry` - An "Authorization Violation" raises AuthenticationException without retrying (single CONNECT) even when reconnect is enabled, leaving state Closed (#46).
- `testConnectIncludesJwtSignatureFromInfoNonce` - JWT auth includes the jwt, the nkey, and a signature of the server nonce ("sig:n-123") in the CONNECT payload.
- `testPublishRequiresOpenConnection` - publish() on a not-open connection throws ConnectionException ("Connection is not open").
- `testDisconnectClosesTransportAndState` - disconnect() closes the transport and sets state to Closed.
- `testSubscribeAndUnsubscribeSendProtocolCommands` - subscribe()/unsubscribe() return SID 1 and emit "SUB orders.created 1" then "UNSUB 1".
- `testUnsubscribeWithMaxDeliversRemainingMessagesThenRemoves` - unsubscribe(sid, 3) after one delivery emits "UNSUB 1 3" and keeps delivering the remaining allowance (m2, m3) to the handler instead of discarding it; the frame past the max (m4) is not delivered (#112).
- `testUnsubscribeWithMaxAlreadyReachedRemovesImmediately` - unsubscribe(sid, 2) after 2 deliveries still emits "UNSUB 1 2" (server-side accounting) but removes the subscription at once, so later frames for the sid are discarded.
- `testReconnectReplaysRemainingAutoUnsubscribeAllowance` - after arming max=3 with 1 delivered, a reconnect replays the SUB and re-arms auto-unsubscribe with the remaining allowance ("SUB updates 1\r\nUNSUB 1 2" as one coalesced replay write, mirroring nats.go resendSubscriptions), so exactly 3 total messages reach the handler (#112, #137).
- `testReconnectCoalescesResubscribeReplayIntoSingleWrite` - a reconnect with 3 subscriptions (one with a #112 remaining allowance armed) replays them as exactly ONE transport write containing every SUB (+UNSUB re-arm) frame in registration order ("SUB orders 1\r\nSUB updates 2\r\nUNSUB 2 3\r\nSUB metrics 3"), and the single post-replay drain still detects a prompt fatal -ERR - the attempt fails and the next attempt recovers with the same single-write replay (#137).
- `testSubscribeRollsBackStateWhenSubWriteFails` - a failed SUB write rethrows and rolls back the registry entry for the consumed sid: frames for that sid are discarded, and the next subscribe gets sid 2 (#116).
- `testUnsubscribeDropsLocalStateWhenUnsubWriteFails` - a failed UNSUB write rethrows but still drops the local subscription state, so no ghost entry survives for resubscribeAll() to revive after recovery (#116).
- `testAutoUnsubscribeCompletesAndCleansUpEvenWhenSlowConsumerDropsMessages` - with maxPending=2 and DropOldest, a single-chunk burst of 4 frames armed at max=4 delivers only the 2 kept messages, yet the subscription is fully torn down because all 4 were received (counting at receive, not at delivery) (#112).
- `testAutoUnsubscribeCapsHandlerDeliveryAtMaxEvenIfServerOverSends` - even when more frames than the armed max arrive on the sid, the handler is invoked at most max times (2) and the subscriptionMeta entry is removed (#112).
- `testAutoUnsubscribeArmedAtOrBelowDeliveredDoesNotOverDeliverQueuedBacklog` - arming unsubscribe(sid, max) at or below the already-delivered count while a message is still queued (sid A arms sid B's cap mid-drain-pass so completeAutoUnsub defers on the non-empty backlog) drops the sid without delivering the queued message, instead of over-delivering exactly one past the cap; the drain loop gates delivery at the top, not just after (#156).
- `testAutoUnsubscribeOnNonOpenConnectionDefersArmingInsteadOfDestroyingSubscription` - unsubscribe(sid, max) while state is not Open (mid-reconnect) neither throws nor destroys the subscription; the max is armed so recovery re-arms it via resubscribeAll (#112).
- `testAutoUnsubscribeArmWriteFailureKeepsSubscriptionArmedForRecovery` - a failed arming UNSUB write propagates but keeps both the subscription and the armed max, so the next recovery can re-arm it (#112/#116).
- `testPlainUnsubscribeOnClosedConnectionDropsStateWithoutThrowing` - plain unsubscribe(sid) for a known sid on a Closed connection releases local state silently instead of throwing (#116).
- `testAutoUnsubscribeCleansUpEvenWhenHandlerThrowsOnMaxDelivery` - a handler that throws on its final (max-th) delivery still leaves the subscription torn down; the terminal cleanup runs in a finally rather than after the handler returns (#112).
- `testSubscribeWithQueueGroupSendsSubFrameAndDeliversToHandler` - subscribe() with a queue group emits "SUB tasks.process workers 1" and dispatches a matching MSG to the handler.
- `testMalformedAsyncInfoDoesNotTearDownTheReadLoop` - A malformed async INFO does not escape processIncoming() or abort the chunk: the bad frame is skipped, the co-chunked MSG is still delivered, and the connection stays Open (#97 dispatch-containment principle).
- `testLargeInboundMessageReceivedWhenServerAdvertisesLargeMaxPayload` - A 9 MiB inbound MSG is delivered intact when the server advertises a 16 MiB max_payload and the connection stays Open (#94).
- `testProcessIncomingDispatchesMsgToSubscriber` - processIncoming() dispatches a MSG to its subscriber, returning 1 frame with correct subject/payload.
- `testDeliveredMessageCanRespondToReplySubject` - A delivered message with a reply subject is replyable and respond() emits one "PUB _INBOX.reply 4\r\npong" frame (#17).
- `testDeliveredMessageCanRespondWithHeaders` - respondWithHeaders() emits an HPUB to the reply subject containing the supplied header (#17).
- `testRespondThrowsWithoutReplySubject` - respond() on a message lacking a reply subject throws LogicException ("no reply subject") and the message is not replyable (#17).
- `testRespondThrowsWhenNotBoundToConnection` - A message constructed outside the delivery path is not replyable and respond() throws LogicException ("not bound to a live connection") (#17).
- `testConnectionListenerReceivesConnectedAndClosed` - The connection listener receives Connected then Closed across connect()/disconnect() (#22).
- `testConnectionListenerReceivesLameDuckAndDiscoveredServers` - An async INFO with ldm+connect_urls emits DiscoveredServers before LameDuck (after Connected) (#22).
- `testErrorListenerReceivesSlowConsumerDrop` - A slow-consumer overflow under DropOldest notifies the error listener once with a "Slow consumer" message (#23).
- `testErrorListenerReceivesRecoverableServerError` - A recoverable -ERR frame keeps the connection Open and notifies the error listener once with a "recoverable error frame" message (#23).
- `testDrainSubscriptionDeliversInFlightThenRemoves` - drainSubscription() UNSUBs, flushes (PING), delivers the in-flight message, then removes the sub so further frames for that sid yield 0 (#43).
- `testConnectionAccessorsAndStatistics` - connectedUrl()/maxPayload() report correctly, statistics() tracks out/in msgs+bytes, and connectedUrl() is null after disconnect (#52).
- `testRttMeasuresPingPong` - rtt() returns a non-negative round-trip time below 5 seconds (#52).
- `testDiscoveredServersFromAsyncInfo` - discoveredServers() is empty initially and reflects connect_urls after an async INFO is processed (#47).
- `testPublishBuffersDuringReconnectAndFlushesOnReconnect` - A publish issued while state is Connecting (mid-reconnect) is buffered (not thrown) and flushed once reconnect completes, incrementing the reconnects stat (#49).
- `testReconnectBufferFlushesMultiplePublishesInOrderBeforeLivePublishes` - Multiple publishes buffered during reconnect are flushed as one ordered block that precedes any post-reconnect live publish (hardening 3b).
- `testReconnectFlushFailureRetainsBufferedPublishesForNextAttempt` - A flush write failure on one reconnect attempt keeps the buffered publishes in place so the next attempt replays them; the frame reaches the wire instead of being silently lost (#123).
- `testPublishDuringRecoverySocketCloseBuffersInsteadOfRacingDeadSocket` - A publish landing while recovery is closing the dead socket takes the reconnect buffer (state flips off Open before recovery's first await) and reaches the wire only after the new handshake (#124).
- `testReconnectExhaustionReportsAbandonedBufferedPublishes` - Exhausting reconnect attempts with a non-empty reconnect buffer surfaces the abandonment through the errorListener and clears the buffer so a later manual connect() cannot replay a dead epoch (#123).
- `testReconnectFlushReachesOpenUnderContinuousPublishPressure` - A fiber publishing continuously during recovery re-fills the reconnect buffer on every flush-write suspension; the bounded flush still reaches Open within a bounded number of publishes (the buffer is SEALED after RECONNECT_FLUSH_MAX_PASSES so late publishers park on the flush gate instead of appending), and a frame buffered before the flip still precedes a post-Open direct frame - where unbounded draining left the connection stuck Connecting for the whole window (#165).
- `testFailedDirectPublishReSendKeepsBufferedBeforeDirectAndPublisherOrder` - When an Open publish's write fails, recovery runs and the frame is re-sent on the fresh socket AFTER the flush (not seeded into it, so recovery success stays independent of the frame per #145 and the post-recovery drain runs first per #144): a frame a concurrent publisher buffered during the outage precedes the re-sent frame (buffered-before-direct), while the re-sending publisher's own later frame follows it (publisher-relative order preserved) (#121).
- `testPublishDuringReplayWindowLandsAfterBufferedPublishes` - A publish issued by a concurrent fiber during the reconnect replay window (handshake done, buffered publishes not yet flushed) keeps buffering and lands AFTER the buffered publishes on the wire, preserving per-publisher ordering (#148).
- `testFailedReplayLegKeepsConnectionRecoveringDuringBackoff` - A reconnect attempt whose replay leg fails after the handshake leaves state off Open with no armed ping timer during the backoff window; a user publish there joins the reconnect buffer and flushes after the next successful attempt (#148).
- `testUserReadDuringReplayWindowDoesNotOverlapRecoveryRead` - A user processIncoming() racing the recovery's own replay read never starts an overlapping transport read (the Amp PendingReadError trigger); it is refused as not-open until the replay completes (#148).
- `testPublishBufferedMidFlushIsFlushedBeforeConnectionOpens` - A publish buffered while the reconnect-buffer flush write is itself suspended is drained by a follow-up flush iteration, in order, before the connection flips Open (#148).
- `testProcessIncomingDispatchesHmsgWithRawHeaders` - An HMSG frame is delivered with rawHeaders and payload kept separate.
- `testProcessIncomingRespondsToServerPing` - A server PING is answered with a protocol PONG (1 frame processed).
- `testSlowConsumerDropOldestPolicy` - With maxPending=1 and DropOldest, only the newest message ("second") is delivered.
- `testSlowConsumerDropNewestPolicy` - With maxPending=1 and DropNewest, only the earliest message ("first") is delivered.
- `testSlowConsumerErrorPolicyThrows` - With maxPending=1 and Error policy, an overflow throws ConnectionException ("Subscription queue overflow").
- `testSlowConsumerErrorOnOneSidDoesNotDiscardSiblingFrames` - An Error-policy overflow on one sid no longer discards same-chunk frames for other sids: siblings are delivered, then the overflow surfaces (#128).
- `testSlowConsumerErrorPolicyOverflowCountsTowardAutoUnsubAndDoesNotLeak` - Under Error policy with maxPending=1 and auto-unsub max=2, a chunk of m1+m2 delivers only m1 (m2 overflows and is dropped) and processIncoming() throws the overflow ConnectionException. Both m1 and m2 still count toward the max at intake (server parity), so the auto-unsub completes below max and the sid is dropped: the subscription does not leak. Rolling back the rejected message's count would strand receivedCounts at 1<2 and leak the sid (#159/#112).
- `testSlowConsumerErrorPolicyOverflowSurfacesToListenerOnceNotTwice` - Under Error policy with maxPending=1, one chunk carrying m1+m2+m3 delivers m1, rethrows m2's overflow to the caller, and reports m3's overflow through the error listener via dispatchFrames' emitErrorSafely - exactly once. The connection layer no longer also emitError()s the same overflow, so the listener receives it once, not twice (#159/#158).
- `testServerPingWithFailingPongWriteStillDeliversCoChunkedMessages` - A failing PONG reply to a server PING does not discard same-chunk MSG frames; the write failure surfaces after dispatch (#128).
- `testHeartbeatReadSurfacesFatalErrAndDeliversCoChunkedMessages` - A fatal -ERR consumed by the heartbeat self-read is no longer swallowed: co-chunked messages are delivered and the error surfaces via the errorListener (#128).
- `testRequestReturnsFirstReplyMessage` - request() returns the first reply payload, writes the shared mux inbox SUB lazily on the first request followed by the PUB carrying the reply subject, and emits no per-request UNSUB (#118).
- `testRequestSurfacesMuxInboxPermissionRejection` - A permissions -ERR rejecting the lazily written mux reply-inbox wildcard SUB makes request() fail fast with a ConnectionException naming the "_INBOX.<inbox>.*" pattern and the "_INBOX.>" permission the account needs, drops the dead mux subscription so no reconnect replays it, and latches so a later request fails at entry without attempting another mux SUB (#167).
- `testUnrelatedSubscriptionPermissionViolationDoesNotLatchMuxRejection` - A recoverable permissions -ERR naming some OTHER subscription does not latch the mux-rejected state: the detection keys strictly on the connection's own random mux base, so request() still returns its reply instead of throwing (#167).
- `testRequestManyReturnsPartialBatchOnMidCollectionMuxRejection` - A mux reply-inbox permission rejection arriving mid-collection makes requestMany() RETURN the replies already collected rather than discarding them by throwing (#167).
- `testRequestManySurfacesMuxInboxPermissionRejectionWhenNothingCollected` - With nothing collected, a mux reply-inbox permission rejection surfaces from requestMany() as the clear ConnectionException naming "_INBOX.>" instead of waiting out the timeout and returning an empty batch: first via the wait-loop throw, then fast at entry once latched, and the entry fast-fail attempts no fresh mux SUB (#167).
- `testRequestReturnsReplyDeliveredOnSameTickAsTimeout` - A reply delivered in the same processIncoming() tick the deadline fires is returned rather than discarded as a timeout (completion-vs-timeout race).
- `testRequestTimesOutWithoutReply` - request() raises TimeoutException ("Request timed out") when no reply arrives, and its post-#118 cleanup writes no per-request UNSUB.
- `testRequestManyCollectsUpToMaxResponses` - requestMany() collects replies up to maxResponses (A,B,C) delivered on the shared mux inbox and emits the PUB with no per-request UNSUB (#21, #118).
- `testRequestManyDoesNotExceedMaxResponsesWhenRepliesArriveInOneChunk` - with maxResponses=2 and three replies (A,B,C) coalesced into ONE chunk, requestMany() returns exactly the first two (`['A','B']`) rather than all three: the inbox collector caps at the limit instead of appending unconditionally (#160).
- `testRequestManyStopsOnStallInterval` - requestMany() with no maxResponses stops after the per-message stall interval, returning the 2 received replies (#21).
- `testRequestManyReturnsEmptyOnNoResponders` - requestMany() returns an empty array on a 503 no-responders status sentinel (#21).
- `testDrainStopsWhenHandlerUnsubscribesItself` - A handler that unsubscribes itself after the first message stops further delivery (only "first" delivered).
- `testServerPoolPreservesOrderWithoutRandomize` - With randomizeServers off, the first dial targets the first configured server (#55).
- `testRandomizeServersDialsFromPool` - With randomizeServers on, the first dial still targets one of the configured pool members (#55).
- `testRetryOnFailedInitialConnectSucceedsAfterRetry` - retryOnFailedInitialConnect retries the first connect (reconnect disabled) until Open, with >= 2 connect attempts (#56).
- `testFailedThenSuccessfulInitialConnectEmitsConnectedNotReconnected` - A failed initial connect() that recovers via recoverConnection() (reconnectEnabled) emits Connected exactly once - no pre-connect Disconnected, no Reconnected - and leaves statistics().reconnects at 0, since a first connect is not a reconnect (#161).
- `testFailedInitialConnectThrowsWithoutRetryOption` - A failed first connect throws ConnectionException when both retryOnFailedInitialConnect and reconnect are off (#56).
- `testTerminalInitialConnectFailureClosesTransportSocket` - A terminal initial-connect failure (reconnect and initial-retry off) closes the transport socket best-effort instead of leaving the dialed fd open until GC (#133).
- `testReconnectExhaustionClosesLastAttemptSocket` - Exhausted recovery closes the LAST attempt's socket (attempts + 1 close calls in total): per-attempt closes happen only at each attempt's start, so the terminal exit must add the final close (#133).
- `testConnectExtractsUrlCredentials` - Credentials in the server URL are stripped from the dialed DSN and applied as user/pass in the CONNECT payload (#37).
- `testRequestUsesConfiguredInboxPrefix` - request() uses the configured inbox prefix for both the SUB and the PUB reply subject.
- `testRequestRejectsNonPositiveTimeout` - request() with timeout 0 throws TimeoutException ("Request timeout must be greater than zero").
- `testRequestCanBeCancelledAndCleansUpSubscription` - request() with a pre-cancelled token throws CancelledException and removes its mux waiter, leaving the shared mux inbox subscribed for reuse (#118).
- `testConcurrentRequestWaiterTakesOverReadPumpAfterFirstReplyArrives` - When the fiber owning the socket read completes its request, a parked waiter takes over the read pump and receives its own reply from a later chunk (no lost wakeup) (#135).
- `testParkedRequestWaiterStillHonorsDeadlineAndCancellation` - A waiter parked behind another fiber's read still times out on its own deadline and observes an external cancellation while parked (#135).
- `testProcessIncomingReconnectsAndResubscribesAfterReadFailure` - A read failure triggers reconnect (2 connect calls) and replays the SUB, then a later MSG is delivered.
- `testMaxPingsOutTriggersReconnect` - With `maxPingsOut: 0` and a fractional (50 ms) ping interval, waits one tick and asserts the ping timer firing forced a reconnect (2 connect calls) landing on server "S2" (relocated from the integration suite - fake transport only, #141).
- `testReconnectAttemptsExhaustedReturnsClosed` - Uses an anonymous transport that fails every reconnect; asserts `processIncoming` throws ConnectionException "Reconnect attempts exhausted" and a subsequent publish throws "Connection is not open" (relocated from the integration suite - fake transport only, #141).
- `testReconnectBackoffDelayProgression` - Anonymous transport fails reconnect attempts 2 and 3 then succeeds on 4; asserts `processIncoming` returns 0, total connect attempts == 4, and the client ends on server "S2" (no wall-clock timing assertion, per #70; relocated from the integration suite - fake transport only, #141).
- `testReconnectAfterMidFramePayloadDropSucceedsOnFirstAttempt` - A connection dying mid-MSG-payload (partial frame in the parser) reconnects on the FIRST attempt and delivers post-reconnect messages: the handshake starts from a clean parser instead of feeding INFO into the stale pending frame (#125).
- `testExhaustedReconnectReleasesStateSoManualReconnectStartsClean` - After reconnect exhaustion, a manual connect() starts from a clean slate: a later recovery replays no SUB from the dead epoch and the old handler never fires again (#127).
- `testDisconnectIsNotReversedByAnInFlightRecovery` - After disconnect(), an in-flight recoverConnection() is a no-op: stays Closed, no Reconnected event, no extra connect, original serverId retained (#84).
- `testPerformRecoveryBailsWhenClosing` - With close-intent already latched, performRecovery() bails without reopening: stays Closed, no Reconnected event, single connect (#84).
- `testDisconnectReleasesSubscriptionAndBufferState` - disconnect() clears subscriptions, subscriptionMeta, pendingMessages, reconnectBuffer, and replaces the parser with a fresh instance (#85).
- `testCallbackMayPublishDuringPostReconnectDeliveryWithoutDeadlock` - A handler that publishes during post-reconnect delivery completes without deadlocking (delivery happens after recovery's critical section), and the ack PUB is written.
- `testProcessIncomingRecoversOnPeerEof` - A graceful peer EOF triggers reconnect + SUB replay, and the next frame is delivered (2 connect calls).
- `testProcessIncomingRecoversOnPeerEofWithPingsDisabled` - With pings disabled, a peer EOF on the read path still triggers reconnect to a healthy server (2 connect calls, stays Open).
- `testProcessIncomingMovesToClosedOnPeerEofWhenReconnectDisabled` - A peer EOF with reconnect disabled leaves the connection Closed with only the single original connect.
- `testReconnectDisabledEofClosesTransportAndReleasesRuntimeState` - A peer EOF with reconnect disabled closes the transport best-effort and clears subscriptions/subscriptionMeta/pendingMessages, keeping the Closed event and the "Reconnect is disabled" exception (#146, #127/#133 invariant).
- `testReconnectDisabledTerminalCloseLetsManualConnectStartClean` - After a reconnect-disabled terminal close, a manual connect() starts clean: a stray MSG carrying the dead epoch's sid is discarded as unknown, never delivered to the stale handler (#146).
- `testPublishRacingTerminalCloseThrowsInsteadOfBufferingSilently` - A publish issued while a terminal path is suspended in its transport-close await (state Closed, reconnecting still set) throws "Connection is not open" instead of buffering bytes that releaseRuntimeState() would silently discard (#146, #123 loud-abandonment invariant).
- `testConsumeHeartbeatResponseRecoversOnPeerEof` - The heartbeat self-read hitting EOF triggers recovery (2 connect calls, stays Open).
- `testConsumeHeartbeatResponseDoesNotRecoverWithoutEof` - A non-EOF empty heartbeat read is swallowed without triggering reconnect (single connect, stays Open).
- `testHeartbeatEofRecoveryStaysOpenWhenPostRecoveryHandlerThrows` - A handler throwing on a message delivered by the post-recovery drain after a heartbeat-EOF recovery leaves the connection Open (not flipped to Closed by consumeHeartbeatResponse's escalation catch), surfaces the handler error via the error listener, and a subsequent publish reaches the new socket (#144).
- `testPostRecoveryContainmentSurvivesThrowingLogger` - The #144 containment holds when the user-supplied PSR logger itself throws while the contained handler error is reported: the connection stays Open after a successful recovery (emitError logs before its listener-throw guard, so the containment wraps it) (#144).
- `testMaxPingsOutRecoveryStaysOpenWhenPostRecoveryHandlerThrows` - The same containment for pingTimerTick's maxPingsOut escalation: a throwing handler on post-recovery delivery leaves the recovered connection Open and the error reaches the error listener (#144).
- `testPublishRetrySucceedsWhenPostRecoveryHandlerThrows` - A publish whose first write fails recovers, the post-recovery drain invokes a throwing handler, and the retried write still runs and succeeds - the caller gets no unrelated handler exception; the handler error surfaces via the error listener only (#144).
- `testConnectRotatesServersOnReconnectAttempts` - A failed first connect rotates to the next configured server on the retry, dialing both pool members in order.
- `testProcessIncomingHandlesServerPongSilently` - A server PONG is consumed without error and the connection stays Open.
- `testProcessIncomingUpdatesServerInfoFromAsyncInfoFrame` - An async INFO refreshes serverInfo (max_payload 64->128, version bump) during an open connection.
- `testProcessIncomingIgnoresRecoverableServerErrFrame` - A recoverable publish-permissions -ERR is processed without closing the connection.
- `testPingTimerSendsPingAtInterval` - The ping timer writes a "PING" frame after the configured interval elapses.
- `testAbandonedOpenConnectionIsCollectedAndClosesItsSocket` - Dropping the last reference to an Open connection frees the object graph (the ping timer holds it only weakly) and the destructor closes the socket best-effort (#126).
- `testPingTimerDisabledWhenIntervalIsZero` - With pingIntervalSeconds 0, no PING is written after connect.
- `testDisconnectCancelsPingTimer` - disconnect() cancels the ping timer so no PING is sent afterward and state is Closed.
- `testPingTimerClosesWhenMaxPingsExceededAndReconnectFails` - With maxPingsOut 0 and reconnect disabled, exceeding outstanding pings closes the connection.
- `testPublishRejectsOversizedPayload` - publish() throws ProtocolException when payload size exceeds server max_payload (65 > 64).
- `testPublishAcceptsPayloadAtExactLimit` - publish() succeeds when payload size equals max_payload (64) and writes a "PUB ... 64" frame.
- `testPublishWithHeadersRejectsOversizedTotal` - publishWithHeaders() validates headers+payload total against max_payload and throws ProtocolException when exceeded.
- `testPublishWithHeadersThrowsWhenServerLacksHeadersSupport` - publishWithHeaders() against an INFO advertising "headers":false throws ConnectionException client-side and no HPUB reaches the wire (nats.go ErrHeadersNotSupported parity, #132).
- `testRequestWithHeadersThrowsWhenServerLacksHeadersSupport` - requestWithHeaders() shares the HPUB capability guard: throws ConnectionException and writes no HPUB against a headers-disabled server (#132).
- `testConnectPayloadIncludesNoResponders` - The CONNECT payload includes "no_responders":true.
- `testRequestThrowsOnNoRespondersStatus` - request() throws NatsException ("No responders for subject ...") on a 503 No Responders HMSG.
- `testPublishRejectsEmptySubject` - publish() with an empty subject throws ProtocolException ("Subject must not be empty").
- `testPublishRejectsSubjectWithWhitespace` - publish() with whitespace in the subject throws ProtocolException ("Subject must not contain whitespace").
- `testPublishRejectsWildcardSubject` - publish() with a "*" wildcard subject throws ProtocolException ("Wildcards are not allowed in publish subjects").
- `testPublishRejectsEmptyTokenInSubject` - publish() with an empty token ("foo..bar") throws ProtocolException ("Subject must not contain empty tokens").
- `testPublishSubjectCacheDoesNotBypassValidationForNewInvalidSubject` - After a valid subject is cached in the validated-subject memo (#136), including a repeat publish hitting the memo, a NEW invalid subject ("cache..invalid") still throws ProtocolException - the memo is keyed per subject and never bypasses validation.
- `testPublishRejectsFullWildcardToken` - publish() with a ">" token throws ProtocolException ("Wildcards are not allowed in publish subjects").
- `testSubscribeAcceptsWildcardSubject` - subscribe() accepts "*" and ">" wildcard subjects, returning sequential SIDs 1 and 2.
- `testSubscribeRejectsGreaterThanNotInLastToken` - subscribe() with ">" not in the last token throws ProtocolException ("Wildcard \">\" must be the last token").
- `testSubscribeRejectsPartialWildcardToken` - subscribe() with a partial wildcard token ("foo.ba*") throws ProtocolException ("Wildcards must occupy an entire token").
- `testDrainUnsubscribesAllAndCloses` - drain() UNSUBs all subscriptions, sends PING, closes the transport, and sets state Closed.
- `testSubscriptionDispatchIsNotReentrantWhenHandlerAwaits` - A handler that awaits a self-pumping request() does not re-enter for the same sid; ordering is start:A,end:A,start:B,end:B (per-sid re-entrancy guard).
- `testLoggerCapturesLifecycleEvents` - An injected PSR-3 logger records Connected/Closed/DiscoveredServers/LameDuck/Disconnected/Reconnected at info level and a per-attempt backoff warning (#69).
- `testFlushSendsPingAndResolvesOnPong` - flush() writes a PING and resolves on the server PONG, staying Open.
- `testDrainedSubscriptionQueueIsKeptEmptyForReuseAndDroppedWithTheSubscription` - After delivery, the per-SID pendingMessages queue is kept empty and the same instance is reused for the next delivery (no alloc/free per message, #139); it is removed with the subscription on unsubscribe.
- `testDrainDoesNotResurrectConnectionOnReadFailure` - drain() with a read failure (EOF) during flush closes without reconnecting or re-subscribing (one CONNECT, one SUB, plus UNSUB).
- `testDrainTerminatesViaDeadlineWhenNoFlushPongArrives` - drain() with no flush PONG ends via its deadline (yielding between empty reads) and closes rather than busy-spinning.
- `testDrainRequiresOpenConnection` - drain() on a not-open connection throws ConnectionException ("Connection is not open").
- `testDrainDeliversBufferedMessagesBeforeClosing` - drain() flushes the in-flight delivery ("hello") to the handler before closing.
- `testDrainWaitsForSuspendedDispatchToDeliverQueuedBacklog` - (#149) a handler that awaits mid-dispatch on another fiber (its sid guarded, a second message still queued) still receives that queued message ("B") before drain() closes; drain() waits (bounded by the single drain deadline) for the in-flight dispatch to finish instead of clearing state and losing the remainder.
- `testDrainDeliversPublishingHandlerBacklogAndReachesClosed` - (#150) a handler that publishes an ack is delivered by drain()'s FLUSH-PHASE read (processIncoming's post-chunk finally-drain, not the dedicated backlog loop) while state is Draining; the publish writes to the wire instead of throwing, and drain() reaches Closed.
- `testDrainContainsThrowingHandlerAndReachesClosed` - (#150) a handler that throws is delivered by drain()'s FLUSH-PHASE read (not the dedicated backlog loop); the throw is contained by the flush-phase catch and surfaced to the error listener, and drain() still tears down to Closed rather than stranding in Draining.
- `testDrainEmitsLoudErrorNamingUndeliveredCountWhenDeadlineExceeded` - (#149 acceptance criterion 2) a handler that stays suspended PAST the drain deadline forces drain() to break the backlog wait with a message still queued behind it; that message cannot be delivered (the registry is cleared and the resumed dispatch loop breaks on the missing subscription), so drain() emits a loud error naming the undelivered count ("drain deadline exceeded: 1 buffered message(s)...") before releasing state - the discard is never silent - and still reaches Closed.
- `testDrainDedicatedLoopSendsAckAndContainsThrowAfterGuardHolderAborts` - (#150) genuinely exercises drain()'s DEDICATED backlog loop and its per-pass containment: a guard-holding dispatch aborts on resume, leaving an ack-publishing handler ("B") and a throwing handler ("C") queued for the dedicated loop to deliver; B's ack reaches the wire while Draining, C's throw is contained by the per-pass catch and surfaced to the error listener, and drain() reaches Closed. Removing that per-pass try/catch lets C's throw escape drain() (stranded in Draining) and fails this test.
- `testRequestTimeoutPreservesOriginalExceptionDuringCleanup` - A request() timeout surfaces TimeoutException ("Request timed out") while the cleanup still runs, dropping the timed-out request's mux waiter and writing no UNSUB (#118).
- `testMalformedHmsgTriggersRecoveryInsteadOfEscaping` - A corrupt HMSG (headerBytes > totalBytes) routes through the recovery path; with reconnect disabled it surfaces as ConnectionException and closes.
- `testParseFailureDeliversSiblingFramesEmitsErrorAndRecovers` - A single chunk of `[valid MSG][garbage line]` delivers the parsed MSG to its handler before recovery, surfaces the ProtocolException through the error listener, and still reconnects with the SUB written exactly twice (initial subscribe + replay) (#147).
- `testHandlerThrowOnRecoveredSiblingFrameStillEmitsErrorAndRecovers` - A handler that throws while the recovered sibling MSG is delivered does not suppress the ProtocolException recovery path: the error listener still observes the parse failure, the connection still reconnects, and the handler's own exception propagates to the caller afterwards (#147).
- `testHeartbeatReadParseFailureDeliversSiblingFramesAndSurfacesError` - The corrupt `[valid MSG][garbage line]` chunk arriving during the heartbeat self-read still delivers the parsed MSG and surfaces the ProtocolException via the error listener, without triggering recovery from the heartbeat path (#147).
- `testReplayParseFailureRetainsSiblingFramesForPostRecoveryDelivery` - A corrupt chunk read by the reconnect subscription-replay poll fails that attempt but enqueues the parsed MSG before the retry replaces the parser, so the post-recovery drain delivers it once the retry succeeds (#147).
- `testBackoffDelayIsExponential` - backoffDelayMs() doubles per attempt (100,200,400,800,1600,3200) and caps at reconnectMaxDelayMs (5000).
- `testRequestWithHeadersReturnsReply` - requestWithHeaders() emits an HPUB carrying the header and returns the first reply payload.
- `testProcessIncomingRequiresOpenConnection` - processIncoming() on a not-open connection throws ConnectionException ("Connection is not open").
- `testUnsubscribeOnUnopenedConnectionIsSilentNoOp` - unsubscribe() on a not-open connection is a silent no-op (state stays Idle, nothing thrown): finally-based inbox cleanup runs on broken connections, and a throw here would leak the entry and mask the caller's original error (#116).
- `testPublishWithHeadersRequiresOpenConnection` - publishWithHeaders() on a not-open connection throws ConnectionException ("Connection is not open").
- `testProcessIncomingThrowsOnErrFrame` - A fatal -ERR frame during processIncoming() throws ConnectionException ("Server sent error frame").
- `testConnectUsesDefaultServerWhenListEmpty` - With an empty servers list, connect() dials the default tcp://127.0.0.1:4222.
- `testSubscribeRejectsEmbeddedWildcardToken` - subscribe() with an embedded wildcard token ("orders.a*") throws ProtocolException ("Wildcards must occupy an entire token").
- `testPublishRecoversAndRetriesAfterWriteFailure` - A PUB write failure triggers reconnect and the publish is retried successfully (2 connect calls, PUB eventually written).
- `testPublishWithHeadersRecoversAndRetriesAfterWriteFailure` - An HPUB write failure triggers reconnect and the header publish is retried successfully (2 connect calls, HPUB eventually written).
- `testPingTimerReconnectsWhenMaxOutstandingPingsExceeded` - With maxPingsOut 0 and reconnect enabled, the ping watchdog reconnects (2 connect calls, stays Open).
- `testReconnectRetriesWhenResubscribeGetsFatalServerError` - A reconnect whose resubscribe hits a fatal auth -ERR retries onto the next server until success (3 connect calls, 3 SUB replays, latches S3).
- `testPingTimerWriteFailureReconnectsWhenEnabled` - A failed PING write triggers a reconnect when reconnect is enabled (2 connect calls, stays Open).
- `testConnectHandlesFragmentedInfoFrame` - An INFO frame split across two reads (xkey, no CRLF in first) is buffered and completes the handshake (NATS 2.10+).
- `testConnectHandlesNonFragmentedReInfoDuringPongPhase` - A complete re-INFO during the PONG phase is applied (max_payload updated to 2097152) and the handshake succeeds.
- `testConnectHandlesFragmentedReInfoDuringPongPhase` - A fragmented re-INFO during the PONG phase is buffered (not raw-parsed) and the handshake succeeds.
- `testPublishRejectsReplyToWithCrlfInjection` - publish() with a CRLF-injected replyTo throws ProtocolException ("Subject must not contain whitespace") (P0-3).
- `testPublishWithHeadersRejectsReplyToWithCrlfInjection` - publishWithHeaders() with a CRLF-injected replyTo throws ProtocolException ("Subject must not contain whitespace") (P0-3).
- `testPublishAcceptsValidReplyTo` - publish() with a valid replyTo emits "PUB orders.created _INBOX.reply.1 4\r\ndata".
- `testSubscribeRejectsQueueGroupWithWhitespace` - subscribe() with whitespace/CRLF in the queue group throws ProtocolException ("Queue group must not contain whitespace") (P0-3).
- `testSubscribeRejectsEmptyQueueGroup` - subscribe() with an empty queue group throws ProtocolException ("Queue group must not be empty").
- `testRequestTimeoutCancelsReadAndAllowsSubsequentRequest` - A request() timeout cancels the underlying read (no orphan), and a subsequent request succeeds with at most one concurrent read and no reconnect (P0-1).
- `testIdleConnectionStaysOpenViaHeartbeatSelfRead` - An idle connection (no processIncoming() calls) stays Open via the heartbeat self-read consuming PONGs, with >= 2 pings sent (P0-2).
- `testHeartbeatResponseDeliversBufferedMessageImmediately` - consumeHeartbeatResponse() delivers a message captured during the self-read immediately (via drainAllPending) rather than leaving it buffered.
- `testProcessIncomingSkipsWhenAnotherReadIsInProgress` - processIncoming() reports 0 frames and starts no overlapping read when readInProgress is already set.
- `testHeartbeatReadSkippedWhenAnotherReadIsInProgress` - The heartbeat self-read yields (no collision) when readInProgress is already set, staying Open.
- `testHeartbeatReadHandlesEmptyErrorAndFatalFrames` - consumeHeartbeatResponse() swallows empty reads, transient read errors, and fatal -ERR frames without closing the connection.
- `testProcessIncomingResetsPingCounterOnlyOnPong` - A data frame does not reset the outstanding-ping watchdog counter; only an actual PONG resets it to 0.
- `testDrainContinuesPastTransientEmptyReadUntilPong` - drain()'s flush ignores a transient 0-frame read, still delivers a server-flushed message ("abc"), and ends only on the PONG.
- `testStandardTlsUpgradeRunsAfterInfoWhenNotHandshakeFirst` - With tls_required and handshake-first off, exactly one explicit post-INFO TLS upgrade runs and TLS becomes active (P1-4).
- `testServerRequiresTlsButNoMaterialsFailsBeforeWritingConnect` - Server requires TLS but client has no materials: fails with a TLS ConnectionException and never writes CONNECT/PING over plaintext, leaving Closed.
- `testServerRequiresTlsUpgradesThenSendsConnect` - Server requires TLS and materials exist: one upgrade runs, TLS goes active, and CONNECT is written only after TLS.
- `testHandshakeFirstDoesNotCallExplicitUpgrade` - Handshake-first negotiates TLS during connect(), so no explicit post-INFO upgrade is called and TLS is active.
- `testHandshakeFirstWithoutEstablishedTlsFailsBeforeWritingConnect` - Handshake-first that fails to establish TLS while server requires it fails with a TLS ConnectionException, writes no CONNECT/PING, and makes no explicit upgrade.
- `testPlainConnectionDoesNotUpgradeTls` - A plain connection performs no TLS upgrade and stays Open.
- `testTlsContextForcesUpgradeEvenWhenServerDoesNotAdvertiseTlsRequired` - A configured tlsContext forces a TLS upgrade before CONNECT even when the server does not advertise tls_required (#95).
- `testTlsContextWithoutEstablishedTlsFailsBeforeWritingConnect` - A configured tlsContext that cannot establish TLS fails the credentials fail-safe (no CONNECT/PING written) and leaves Closed (#95).
- `testRttThrowsWhenConnectionNotOpen` - rtt() on a not-open connection throws ConnectionException ("Connection is not open").
- `testDrainSwallowsFatalFrameErrorMidFlush` - drain() swallows a fatal -ERR arriving mid-flush and still closes cleanly.
- `testPublishWithHeadersBuffersDuringReconnectAndRecordsOutbound` - publishWithHeaders() during reconnect buffers the HPUB (not thrown), records outbound stats immediately, and flushes the HPUB on reconnect.
- `testPublishBufferOverflowThrowsDuringReconnect` - With a 1-byte reconnect buffer, a publish during reconnect overflows bufferFrame() and throws ConnectionException ("Connection is not open").
- `testSubscribeThrowsWhenConnectionNotOpen` - subscribe() on a not-open connection throws ConnectionException ("Connection is not open").
- `testDrainSubscriptionOnClosedConnectionDropsStateAndReturns` - drainSubscription() on a closed connection drops local state and returns without throwing.
- `testDrainSubscriptionOnUnknownSidReturnsEarly` - drainSubscription() with an unknown SID returns early, emitting no extra wire commands.
- `testDrainSubscriptionSwallowsFlushFailureAndDropsSub` - drainSubscription() swallows a flush timeout and still removes the subscription from subscriptionMeta.
- `testFlushThrowsWhenConnectionNotOpen` - flush() on a not-open connection throws ConnectionException ("Connection is not open").
- `testFlushTimesOutWhenNoPongArrives` - flush() throws TimeoutException ("Flush timed out") when the server PONG never arrives.
- `testRequestThrowsWhenConnectionNotOpen` - request() on a not-open connection throws ConnectionException ("Connection is not open").
- `testRequestManyThrowsWhenMaxResponsesLessThanOne` - requestMany() with maxResponses 0 throws InvalidArgumentException ("maxResponses must be at least 1").
- `testRequestManyThrowsWhenStallMsNotPositive` - requestMany() with stallMs 0 throws InvalidArgumentException ("stallMs must be greater than zero").
- `testRequestManyThrowsWhenConnectionNotOpen` - requestMany() on a not-open connection throws ConnectionException ("Connection is not open").
- `testRequestManyThrowsWhenTotalTimeoutIsZero` - requestMany() with totalTimeoutMs 0 throws TimeoutException ("Request timeout must be greater than zero").
- `testRequestManyWithHeadersUsesHpub` - requestMany() with headers publishes via HPUB (not PUB) and collects the reply.
- `testConnectSeedsKnownConnectUrlsFromInitialInfo` - connect() seeds discoveredServers from the initial INFO connect_urls.
- `testServerPoolNormalizesAndDeduplicatesDiscoveredUrls` - serverPool() includes the configured server plus discovered peers, normalizes bare host:port to nats://, and deduplicates repeated URLs.
- `testRetryInitialConnectFailsFastOnAuthError` - retryInitialConnect() fails fast on an auth -ERR (AuthenticationException) without exhausting all attempts (one connect call), leaving Closed.
- `testRetryInitialConnectReturnsFalseWhenExhausted` - retryInitialConnect() returning false after all attempts fail makes connect() throw ConnectionException and leaves Closed.
- `testReconnectFailsFastOnAuthDuringReconnect` - An auth -ERR during a reconnect attempt stops the reconnect loop, emits Closed, and limits to 2 connect calls (initial + one reconnect).
- `testDrainImmediateServerFramesHandlesOkAndTimeout` - On reconnect, drainImmediateServerFrames() skips a +OK frame and returns on a poll-timeout CancelledException, staying Open (2 connect calls).
- `testConnectFailsWhenErrArrivesInsteadOfInfo` - awaitServerInfo() throws ConnectionException ("Server error during connect") when an -ERR arrives as the first frame instead of INFO.
- `testConnectHandlesExpiredDeadlineDuringHandshakeRead` - Repeated empty reads exhaust the handshake budget, exercising the expired-deadline path and throwing ConnectionException ("Expected PONG after CONNECT").
- `testProcessIncomingTreatsInvalidSubjectErrAsRecoverable` - An "Invalid Subject" -ERR is treated as recoverable: connection stays Open and the error listener is notified once.
- `testConsumeHeartbeatResponseMarksClosedWhenRecoveryFails` - consumeHeartbeatResponse() reading EOF with reconnect disabled catches the recovery failure and marks state Closed.
- `testConnectionListenerExceptionIsSwallowed` - An exception thrown by the connection listener (on Connected and Closed) is swallowed; disconnect() still completes Closed.
- `testErrorListenerExceptionIsSwallowed` - An exception thrown by the error listener is swallowed; the listener is invoked exactly once and the connection stays Open.
- `testHandleServerInfoUpdateNoopsWhenServerInfoIsNull` - handleServerInfoUpdate() returns early (no throw) when serverInfo is null, staying Open.
- `testLameDuckWithFailoverEmitsErrorWhenRecoveryFails` - After discovering a peer (pool of 2), a lame-duck INFO emits LameDuck and triggers a failover that fails, invoking the error listener.
- `testDrainPendingForSidRemovesPendingWhenSubscriptionIsGone` - drainPendingForSid() unsets the pendingMessages entry when the subscription handler is gone.
- `testRequestManyRethrowsExternalCancellation` - requestMany() with a pre-cancelled external token rethrows CancelledException.
- `testStatisticsTracksOutboundCountsForHeaderPublish` - statistics() increments outMsgs to 1 and outBytes to the payload length after a publishWithHeaders().
- `testRequestManyInternalContinuesOnSliceTimeout` - requestMany() with one reply and a stall window takes the `continue` branch on internal slice timeouts (no external cancel) and returns ['A'].
- `testRequestManyRethrowsExternalCancellationFromProcessIncoming` - requestMany() rethrows CancelledException when an external cancellation fires while processIncoming() is suspended in the loop.
- `testRecoverConnectionCoalescesConcurrentCallers` - Two concurrent recoverConnection() callers coalesce onto one in-progress reconnect (total 2 connect calls, stays Open).
- `testRetryInitialConnectIgnoresCloseFailureBetweenAttempts` - retryInitialConnect() swallows a throwing transport.close() between attempts and the next connect attempt still succeeds to Open (>= 2 connect calls).
- `testPerformRecoveryIgnoresCloseFailureDuringReconnect` - performRecovery() swallows a throwing transport.close() during the reconnect loop after a peer EOF and still reconnects to Open (2 connect calls).
- `testConnectDuringInFlightRecoveryJoinsInsteadOfSecondDialChain` - connect() while a recovery is suspended mid-attempt joins the recovery instead of starting a second dial chain: total dials stay at the recovery's own count (3, not the 4 of the parallel-dial bug), the connection ends Open, and a subscription created right after the joined connect() returns survives and receives its message (#145).
- `testConnectDuringDrainThrowsWithoutDialing` - connect() while drain() is mid-flush (state Draining) throws ConnectionException without dialing (single transport connect), and drain still completes to Closed (#145).
- `testConnectDuringRecoveryDoesNotDisarmConcurrentDisconnectCloseIntent` - disconnect() during an in-flight recovery followed by a racing connect(): the connect() joins instead of resetting close-intent and dialing, the resumed recovery bails on `closing`, the join surfaces the abort as ConnectionException ("aborted before the connection opened") instead of resolving as success, the connection stays Closed with no Reconnected event and no extra dial, and a later manual connect() still opens a clean epoch (#145).
- `testConnectAwaitedFromDisconnectedListenerDuringRecoveryThrowsInsteadOfDeadlocking` - connect()->await() from a Disconnected connection listener (which runs inside the recovery fiber) throws ConnectionException ("cannot join the in-flight recovery") instead of joining and awaiting the recovery's own deferred from the only fiber that can complete it - a permanent deadlock pre-fix; the recovery itself still completes normally (Open, 1 reconnect, one dial chain), with the pump awaited under a TimeoutCancellation so a regression fails instead of hanging the suite (#145).
- `testStaleFailureContinuationDuringManualConnectDoesNotStartRecovery` - a publish write that suspended before a terminal close and resumes failing while a manual connect() is suspended mid-dial does not start a recovery: no extra transport close (the fresh dial's socket survives), no extra dial, no Disconnected event, and the manual connect() completes to Open with exactly one reconnect dial (#145).
- `testConnectFromClosedListenerAfterTerminalDialFailureThrowsInsteadOfDeadlocking` - connect()->await() from a Closed connection listener during a terminal initial connect (reconnect disabled, every dial refused) throws ConnectionException ("cannot be re-entered from a connection/error listener") instead of joining the still-pending connecting deferred whose only completer is the suspended emitting fiber - a permanent deadlock pre-fix. The outer connect() surfaces the dial failure as ConnectionException (awaited under a TimeoutCancellation so a deadlock regression fails instead of hanging the suite), the connection ends Closed, and the refused re-entrant connect() does not dial a second time (#145).
- `testLiveEpochFailureWhileConnectedListenerParkedStartsRecoveryNotSwallowed` - a publish write failure on the live socket while the Connected listener is still parked mid-emission (state Open, the connect() closure suspended inside the Connected callback) starts a recovery instead of being swallowed: settling the connecting deferred before the Connected emission plus the state != Open clause on recoverConnection()'s in-flight-connect guard let the failure through, so Disconnected + Reconnected fire, the transport dials a second time, and the connection ends Open rather than sitting Open on a dead socket with no reconnect (#145).
- `testOwnerConnectAbortedByConcurrentDisconnectThrowsInsteadOfResolvingSuccess` - an owner connect() whose initial dial fails and hands off to an owned recovery that a concurrent disconnect() then aborts throws ConnectionException ("aborted before the connection opened") instead of resolving as success on a Closed connection: connect() applies the not-Open invariant to the owner exactly as to joiners. The join is awaited under a TimeoutCancellation so a wedge regression fails instead of hanging, and the connection ends Closed (#145).
- `testConcurrentFlushWaitsForItsOwnPongAfterSiblingTimeout` - (#117) with two concurrent flushes, flush A's timeout must not release flush B, and the late PONG answering A's PING must not complete B either; B completes only on the pong answering its own PING.
- `testDrainDeliversMessagesArrivingBetweenStaleHeartbeatPongAndItsOwnPong` - (#117) drain()'s flush-wait is not satisfied by a stale heartbeat PONG (self-read consumed nothing): the MSG arriving between the stale PONG and drain's own PONG is still delivered before the socket closes.
- `testFlushErrorsOutWhenConnectionDropsMidFlush` - (#117) a flush whose socket EOFs before the PONG errors out with ConnectionException when recovery replaces the connection epoch, instead of idling out its deadline on the new socket (recovery itself succeeds, 2 connect calls).
- `testTrailingFrameCoalescedBehindHandshakePongIsProcessed` - (#157) an async INFO the server coalesces behind the handshake PONG in one TCP segment still updates the discovered-server pool (`discoveredServers()` == ['10.0.0.2:4222']) instead of being dropped when awaitInitialPong() returned at the PONG.
- `testPartialFrameBufferedBehindHandshakePongSurvivesParserTransition` - (#157) a frame partly buffered behind the handshake PONG completes on the next read (`discoveredServers()` == ['10.0.0.9:4222']) with no thrown error and no emitted ProtocolException, because the post-handshake bound change reuses the parser instead of discarding its buffer.
- `testLameDuckCoalescedBehindReconnectPongDoesNotDeadlockRecovery` - (#157) a lame-duck INFO coalesced behind the RECONNECT handshake PONG is dispatched during connectOnce and its re-entrant recoverConnection() is a no-op (guarded by the recovery fiber) so the in-flight recovery completes to Open with `discoveredServers()` == ['10.0.0.9:4222'] instead of deadlocking on its own future.
- `testLameDuckDuringReplayPollDoesNotWedgeRecovery` - A lame-duck INFO delivered during the subscription-replay poll (drainImmediateServerFrames, not behind the handshake PONG) triggers recoverConnection() from inside the recovery fiber; the same-fiber guard makes it a no-op so recovery completes instead of self-awaiting forever (#166).
- `testFatalErrorCoalescedWithThrowingHandlerStillSurfaces` - (#158) a fatal -ERR co-chunked with a MSG whose handler throws during the per-chunk drain surfaces as the ConnectionException naming 'Authorization Violation' (not masked by the handler throw), and the handler failure is still emitted through the error listener.
- `testMultipleFrameDispatchFailuresAreAllObservable` - (#158) two frames failing dispatch in one chunk are both observable: the first surfaces as the thrown ConnectionException ('Boom One') and the second is emitted through the error listener ('Boom Two').
- `testTerminalCloseReportsDiscardedInboundBacklog` - (#158) a terminal close (reconnect exhausted) discarding a non-empty parsed inbound backlog emits one error naming the discarded count (3), mirroring the outbound reconnect-buffer discard.
- `testPublishHeaderBlockValidatesEverySubject` - Asserts publishHeaderBlock() validates every message subject up front, aborting the whole block with a ProtocolException on an invalid subject before any bytes are written.
- `testPublishHeaderBlockSplitsSegmentsAtCap` - Asserts a header block larger than the 512 KiB segment cap (three 300 KiB frames) is flushed as three separate segment writes rather than one giant write (the flush-before-overflow segmentation, #138).
- `testPublishHeaderBlockRecordsOutboundPerMessage` - Asserts publishHeaderBlock() records outbound stats once per message: statistics() outMsgs advances by the block size and outBytes by the total payload bytes.
- `testUnsubscribeUnknownSidWritesNoUnsub` - Asserts unsubscribe() on a never-registered sid is a no-op that emits no spurious UNSUB to the server.
- `testConnectAuthFailureClosesTransport` - Asserts an authorization failure during connect releases the transport socket (closed) before the AuthenticationException propagates (#133).
- `testTerminalCloseSumsDiscardedInboundBacklogAcrossSubscriptions` - Asserts the terminal-close inbound-discard report SUMS backlog across subscriptions: two queues holding 2 and 3 undelivered messages are named as 5 discarded, not just the last queue's count (#158).
- `testHasUndeliveredDrainBacklogTrueWhileDispatchInFlight` - Asserts hasUndeliveredDrainBacklog() reports true while a dispatch loop is suspended mid-delivery (dispatchingSids non-empty) even when no sid queue is dirty (#149).
- `testCountUndeliveredDrainBacklogSumsAllQueues` - Asserts countUndeliveredDrainBacklog() sums buffered messages across every sid queue (2 + 3 = 5), the total the loud deadline-exceeded discard count depends on (#149).
- `testResubscribeAllReArmsAutoUnsubWithRemainingAllowance` - Asserts reconnect subscription replay re-arms an auto-unsubscribe with the remaining allowance (max minus received, = full max when nothing received yet) so a fresh SUB does not over-deliver (#112).
- `testResubscribeAllDropsAutoUnsubAtExhaustedMax` - Asserts reconnect subscription replay drops (does not re-SUB) an auto-unsubscribe whose max was already reached (remaining <= 0), avoiding over-delivery of live messages past the max (#112).
- `testDiscardPongSlotRemovesOnlyTheGivenSlot` - Asserts discardPongSlot() removes only the given slot from the pong-correlation queue, preserving the other queued waiter in order (#117).
- `testInboundFrameBoundFallsBackToSixtyFourMiB` - Asserts inboundFrameBound() falls back to exactly 64 MiB when the server advertises no usable max_payload (serverInfo null), preserving large-frame headroom (#94).
- `testConcurrentConnectJoinsTheInFlightDial` - Asserts a second connect() joins the first in-flight dial instead of dialing again: exactly one socket connect and one CONNECT handshake reach the wire, and both callers resolve Open (#145).
- `testTerminalTeardownSurvivesFailingSocketClose` - Asserts a throwing transport close() on the reconnect-disabled terminal path is swallowed: the "Reconnect is disabled" ConnectionException, the Closed state, and the Closed event all still surface unmasked (#133/#146).
- `testPublishHeaderBlockWritesNothingForEmptyBatch` - Asserts publishHeaderBlock() with an empty batch is a wire no-op that writes nothing and validates nothing.
- `testPublishHeaderBlockThrowsWhenServerLacksHeadersSupport` - Asserts publishHeaderBlock() shares the HPUB capability guard: against an INFO advertising "headers":false the whole block fails with a ConnectionException before any HPUB byte reaches the wire (#132).
- `testFlushRethrowsPingWriteFailureAndKeepsPongCorrelationAligned` - Asserts a flush() whose PING write fails outright rethrows the write error, stays Open, and discards that PING's pong slot (nats.go removePongFromList parity), so the next flush is completed by its own PONG instead of a stale head slot eating it.
- `testParseErrorRecoveryRunsEvenWhenLoggerThrows` - Asserts a mid-chunk parse failure still recovers when the user-supplied PSR logger throws while the error is reported: the frame parsed before the corrupt bytes is delivered first and the connection reconnects (#147/#128).
- `testRequestRetriesMuxInboxEstablishmentAfterFailedSubWrite` - Asserts a mux-inbox establishment whose SUB write fails surfaces the transport error through the request and rolls back cleanly, so the next request() re-attempts the establishment and receives its reply, with only the successful SUB on the wire (#118).
- `testRequestManyParkedBehindForeignReadCollectsReplyOnceSlotFrees` - Asserts a requestMany() collector parked behind another fiber's socket read wakes when that read releases the slot, takes over the read itself, and still collects its reply (#135).
- `testRequestManyParkedBehindForeignReadHonorsTotalDeadline` - Asserts a requestMany() collector parked behind another fiber's read still honors its total deadline, returning an empty collection in bounded time without ever taking a socket read of its own or disturbing the still-parked foreign read (#135).
- `testDisconnectDuringReconnectBackoffStopsFurtherAttempts` - Asserts a disconnect() issued while a reconnect sits in its backoff sleep stops the loop at the next attempt gate: attempt 2 never dials, no Reconnected event fires, and the connection stays Closed (#84).
- `testUserCloseDuringReconnectHandshakeAbortsRecoveryEvenWhenCloseFails` - Asserts a disconnect() landing inside a recovery attempt's CONNECT write is not overridden when that handshake completes: the fresh socket is torn straight back down best-effort (its close() even throws), the recovery does not redial, and it never flips Open or emits Reconnected (#84).
- `testUserCloseDuringSubscriptionReplayAbortsRecoveryBeforeOpen` - Asserts a disconnect() landing during the recovery's subscription replay aborts at the post-replay close-intent re-check, tearing the new socket down even when that close throws, with no redial and no Reconnected event (#84).
- `testCorruptReplayResponseFailsTheAttemptAndTheNextAttemptRecovers` - Asserts a replay-response chunk holding a MSG and a fatal -ERR ahead of corrupt tail bytes fails that reconnect attempt (never leaving the connection open on a corrupt stream) while the MSG parsed before the corruption survives the failed attempt and is delivered once the next attempt recovers (#147/#128).
- `testHandshakeToleratesStrayMsgFrameBeforePong` - Asserts a stray MSG arriving before the handshake PONG is tolerated and skipped rather than fatal: the handshake completes on the later PONG and the dropped frame is never dispatched (inMsgs stays 0).
- `testConnectTimesOutAgainstSilentServer` - Asserts connect() against a silent server fails within its connect-timeout budget with "Expected INFO during connect" and leaves the connection Closed, so the bounded per-slice reads cannot hang the dial.
- `testThrowingSubscriptionRejectionHandlerIsContained` - Asserts a per-sid rejection handler that throws while being notified of a permissions -ERR is contained: frame dispatch continues, the recoverable error still reaches the error listener, and the connection stays Open (#167 family).
- `testHeartbeatTickSkipsSelfReadWhenDisconnectLandsDuringPingWrite` - Asserts a disconnect() completing inside the heartbeat tick's PING write suppresses the tick's follow-up self-read entirely, so no socket read starts against a socket the teardown just released (#148).
- `testHeartbeatReadRecoversFromOneShotProtocolViolation` - Asserts a one-shot transport ProtocolException surfacing on the heartbeat timer's read is not swallowed: it reaches the error listener and the connection recovers with a second dial and CONNECT handshake, instead of running a corrupt stream forever.
- `testHeartbeatProtocolViolationForcesClosedWhenRecoveryUnavailable` - Asserts the same heartbeat-read protocol violation with reconnect disabled still reaches the error listener and forces the connection Closed with the Closed event announced, never a silent swallow that leaves a corrupt stream Open.
- `testHeartbeatProtocolViolationRecoveryRunsEvenWhenLoggerThrows` - Asserts the heartbeat protocol-violation branch reports through emitErrorSafely, so a throwing user-supplied PSR logger can neither skip the recovery below it nor escape into the event-loop timer: the connection still redials and reaches Open (#150).
- `testHeartbeatReadSurfacesFatalErrRecoveredFromMidChunkParseFailure` - Asserts a heartbeat-read chunk whose parsed head holds a fatal -ERR plus a MSG and whose tail is corrupt delivers the recovered MSG and surfaces BOTH the parse failure and the fatal frame error via the error listener, without escalating from the timer (#147/#128).
- `testHeartbeatReadContainsThrowingHandlerDuringBacklogDrain` - Asserts a user handler that throws while the heartbeat self-read drains its captured messages is contained by the timer: the message was delivered, nothing escapes the timer, and the connection stays Open and still publishes.
- `testDestructorTeardownSurvivesThrowingClose` - Asserts the destructor's deferred socket teardown for an abandoned Open connection swallows a throwing close() inside the queued closure instead of letting it escape into the event loop (#126).
- `testSubjectValidationMemoResetAtCapKeepsValidating` - Asserts crossing the 512-subject validated-subject memo cap resets the memo without breaking publishing (every publish across the reset reaches the wire) or weakening validation (an invalid subject still throws afterwards) (#136).
- `testConnectReenteredFromClosedListenerThrowsFullOperatorGuidance` - Asserts connect() re-entered from a Closed listener firing on the connecting fiber's own terminal path is refused with the FULL operator guidance, pinned verbatim so no fragment of the diagnosis or the remedy can silently drop (#145).
- `testRandomizeServersShufflesDialOrderDeterministically` - Asserts randomizeServers actually SHUFFLES the configured pool rather than merely dialing a member of it: with the RNG seeded, the first dial matches the seed's shuffle permutation, which moves index 0 (#55).
- `testDestructOnNeverOpenedConnectionLeavesTransportUntouched` - Asserts destructing a connection that never opened does not touch its transport, since the queued teardown is reserved for abandoned Open connections (#126).
- `testAsyncSubscriptionRejectionOnlyNotifiesTheNamedSubjectsHandler` - Asserts an async permissions -ERR naming one subscription's subject notifies ONLY that subject's registered rejection handler, never another sid's, and leaves the connection Open (#167 family).
- `testRequestWithPendingExternalCancellationTimesOutWithTimeoutException` - Asserts a request whose deadline fires while a supplied external cancellation token was never requested surfaces the documented TimeoutException, not the internal CancelledException of the timed-out wait (#135 family).
- `testJoinerOfInFlightConnectFailsWhenDialIsAbortedByDisconnect` - Asserts both the owner and a joining connect() fail with "Connect was aborted before the connection opened" when a concurrent disconnect() aborts the in-flight dial, so supervision code cannot read a resolved connect() as connected (#145).
- `testJoinerSharesExhaustionFailureOfOwnedRecovery` - Asserts an owned recovery that exhausts settles the still-pending connecting deferred with "Reconnect attempts exhausted", so a parked joiner shares the failure instead of hanging forever (#145).
- `testJoinerSharesAuthenticationFailureOfInFlightConnect` - Asserts a joiner parked on an in-flight connect whose handshake fails with an auth error receives that same AuthenticationException and the connection ends Closed (#145/#46).
- `testJoinerSharesTerminalWrappedFailureOfInFlightConnect` - Asserts a joiner parked on an in-flight connect that fails terminally (reconnect disabled) receives the SAME wrapped ConnectionException the owner throws, one error type for every caller of the dial (#145).
- `testConnectFromAnotherFiberJoinsInFlightRecoveryAndSharesSuccess` - Asserts connect() issued from a non-recovery fiber joins an in-flight recovery and shares its successful outcome without starting a third dial, since only a call from inside the recovery fiber itself is refused (#145).
- `testTrailingHandshakeFramesAreDeliveredEvenWhenTheAttemptFails` - Asserts a MSG the server coalesced behind the reconnect-handshake PONG reaches its handler even when a fatal -ERR in the same batch fails the attempt and the recovery then exhausts, with nothing reported as discarded (#157/#147).
- `testFramesRecoveredFromAParseFailureAreDeliveredBeforeRecoveryRuns` - Asserts frames recovered from a mid-chunk parse failure are delivered BEFORE the recovery dials (still on the failing epoch) even when one recovered frame is a fatal -ERR, so the post-recovery drain cannot reorder them against the wire (#147).
- `testRecoveryAbortedByDisconnectAfterAFreshDialClosesTheNewSocket` - Asserts a recovery attempt whose dial succeeded but whose epoch a concurrent disconnect() aborted closes the freshly dialed socket and does not count the aborted attempt as a reconnect (#84/#133).
- `testRecoveryAbortedByDisconnectDuringReplayClosesTheNewSocket` - Asserts close-intent set during the subscription replay also tears the new socket down at the post-replay closing re-check instead of flipping Open (#84).
- `testAuthFailureDuringRecoveryReportsAndDiscardsSuspendedBacklogAndClosesSocket` - Asserts a terminal auth failure during recovery releases the freshly dialed socket (#133), reports the parsed-but-undelivered inbound backlog loudly before clearing it (#158), and releases runtime state so a dispatch suspended mid-delivery cannot deliver into the dead epoch afterwards (#127/#46).
- `testRetryInitialConnectAuthFailureClosesSocketAndSettlesJoiner` - Asserts the initial-connect retry loop's auth fail-fast releases the dialed socket and settles a parked joiner with the same AuthenticationException (#46/#133/#145).
- `testRetryInitialConnectWaitsTheBackoffBeforeRedialing` - Asserts the initial-connect retry loop waits its configured backoff before redialing rather than hammering the server with immediate retries (#56).
- `testSecondFailingPublisherAwaitsInFlightRecoveryAndRetriesOnTheFreshSocket` - Asserts a publish whose write fails while another fiber's recovery is already in flight awaits that recovery and retries on the fresh socket, so both publishes land instead of one failing spuriously against the dead socket.
- `testPingTimerTickOnNotOpenConnectionWritesNothing` - Asserts a heartbeat tick on a connection that is not Open writes nothing, so its PING cannot inject bytes into a handshake or teardown the tick does not own.
- `testPublishHeaderBlockSegmentCapBoundary` - Asserts the 512 KiB publishHeaderBlock() segment cap is a strict greater-than boundary: two frames summing EXACTLY to the cap share one transport write, and one byte past it forces a flush into two writes.
- `testReplayImmediateFramePollStopsAfterExactlySixteenChunks` - Asserts the reconnect replay's immediate-frame poll consumes exactly 16 prompt chunks and then moves on, pinning the loop bound so the replay window cannot silently widen or shrink.
- `testReplayImmediateFramePollTimesOutPromptlyOnAnIdleSocket` - Asserts each replay poll is bounded to roughly 5 ms so an idle socket ends it almost immediately, keeping recovery fast where a mis-scaled poll timeout would stall it.
- `testAutoUnsubBacklogSurvivesAHandlerThrowAndIsDeliveredBeforeTheDrop` - Asserts an auto-unsubscribe whose server-side max is already received waits for the queued backlog before dropping the sid: a message counted toward the max but left queued by a throwing handler is still delivered on the next drain pass (#112/#162).
- `testMissedPongEscalationDoesNotAlsoSendTheTickPing` - Asserts a heartbeat tick that trips maxPingsOut escalates into recovery and then stops, leaving only CONNECT plus PING per epoch on the wire so no stray tick heartbeat is injected into the fresh epoch.
- `testHeartbeatTickDoesNotStartASecondReadWhileAUserReadIsInFlight` - Asserts a heartbeat tick firing while a user read owns the socket sends its PING but starts no second overlapping transport read (#148 read-slot discipline).
- `testPublisherParkedOnTheFlushGateWritesOnceTheGateOpensOnAnOpenConnection` - Asserts a publisher parked on the sealed reconnect-flush gate completes its write exactly once when the gate opens with the connection Open, instead of falling through into the buffering path and failing a publish whose frame was just written (#165).
- `testRequestManyExternalCancellationSurfacesPromptly` - Asserts an external cancellation of requestMany() surfaces promptly rather than only once the total deadline eventually expires (#135 family).

### tests/Unit/NatsHeadersTest.php
- `testToWireBlockEmitsRepeatedLinesForListValue` - Asserts a list-valued header (`Link` => [a.txt, b.txt]) encodes to one `Link:...\r\n` line per element plus the scalar `Nats-Msg-Id:1\r\n` line (multimap encoding, #42).
- `testFromWireBlockMultiPreservesAllValues` - Asserts `fromWireBlockMulti` returns all repeated values as a list (`Link`=>[a.txt,b.txt], scalar wrapped as [one]) while `fromWireBlock` is last-value-wins (`Link`=>b.txt).
- `testFromWireBlockMultiParsesStatusLine` - Asserts a `NATS/1.0 503 No Responders` status line parses into single-element lists `Status`=>['503'] and `Description`=>['No Responders'].
- `testRoundTripWireEncoding` - Asserts `toWireBlock` output starts with `NATS/1.0\r\n`, ends with `\r\n\r\n`, and round-trips back through `fromWireBlock` to the original header map.
- `testFromWireBlockSkipsMalformedHeaderLines` - Asserts `fromWireBlock` returns [] for null/empty input and skips lines with no separator or empty name, keeping only the valid `Valid` header.
- `testFromWireBlockParsesStatusLine` - Asserts a `NATS/1.0 100 Idle Heartbeat` block parses into `Status`, `Description`, and the following `Nats-Consumer-Stalled` header.
- `testToWireBlockRejectsEmptyHeaderName` - Asserts `toWireBlock` throws InvalidArgumentException ('Header name') for an empty-string header name.
- `testToWireBlockRejectsHeaderNameWithColonOrWhitespace` - Asserts `toWireBlock` throws InvalidArgumentException ('Header name') for a name containing a colon (`a:b`).
- `testHeaderValueIsEmittedVerbatimOnTheWire` - Asserts the wire carries the caller's value bytes untouched (`X-Test:  spaced  `) for nats.go parity so signature-carrying values are never mutated, while this client's inbound decode still trims to `spaced`.
- `testGetLooksUpHeaderNamesCaseInsensitively` - Asserts `NatsHeaders::get()` resolves a name case-insensitively (`Nats-Msg-Id` finds a publisher's lowercase `nats-msg-id`), still returns exact-case matches, and yields null for an absent name.
- `testToWireBlockRejectsHeaderValueWithCarriageReturn` - Asserts `toWireBlock` throws ('Header values must not contain CR or LF characters') when a value contains a CR.
- `testToWireBlockRejectsHeaderValueWithLineFeed` - Asserts `toWireBlock` throws the same CR/LF error when a value contains an LF.
- `testToWireBlockRejectsMultiValueListWithCrLfInElement` - Asserts `toWireBlock` throws the CR/LF error when any element of a multi-value list contains CR/LF (header injection guard).
- `testFromWireBlockMultiReturnsEmptyForNull` - Asserts `fromWireBlockMulti(null)` returns [].
- `testFromWireBlockMultiReturnsEmptyForEmptyString` - Asserts `fromWireBlockMulti('')` returns [].
- `testFromWireBlockMultiSkipsLinesWithoutColon` - Asserts `fromWireBlockMulti` omits a colon-less line and still parses the valid `Valid`=>['good'] header.
- `testFromWireBlockMultiSkipsLinesWithEmptyName` - Asserts `fromWireBlockMulti` omits a line whose name is empty after trimming and keeps `Valid`=>['present'].
- `testFromWireBlockMultiAccumulatesRepeatedHeaderLines` - Asserts three raw `Link:` lines accumulate into `Link`=>['first','second','third'] (multimap behaviour from raw wire input).
- `testFromWireBlockMultiStopsAtEmptyLine` - Asserts `fromWireBlockMulti` stops at the blank line, keeping `Before`=>['yes'] and ignoring the post-block `After` header.
- `testFromWireBlockMultiParsesStatusLineWithoutDescription` - Asserts a `NATS/1.0 404` status-only line yields `Status`=>['404'] and no `Description` key.

### tests/Unit/NatsOptionsTest.php
- `testFirstServerReturnsConfiguredFirstEndpoint` - Asserts `firstServer()` returns the first configured server (`nats://a:4222`) from the servers list.
- `testFirstServerFallsBackWhenServersListIsEmpty` - Asserts `firstServer()` returns the default `nats://127.0.0.1:4222` when the servers list is empty.
- `testRejectsNonPositiveConnectTimeout` - Asserts the constructor throws InvalidArgumentException ('connectTimeoutMs') for `connectTimeoutMs: 0`.
- `testRejectsNonPositiveRequestTimeout` - Asserts the constructor throws InvalidArgumentException ('requestTimeoutMs') for `requestTimeoutMs: 0`.
- `testRejectsZeroMaxPendingMessages` - Asserts the constructor throws InvalidArgumentException ('maxPendingMessagesPerSubscription') for value 0.
- `testRejectsNegativeReconnectValue` - Asserts the constructor throws InvalidArgumentException ('reconnectDelayMs') for `reconnectDelayMs: -1`.
- `testRejectsNonPositiveReadChunkSize` - Asserts the constructor throws InvalidArgumentException ('readChunkSizeBytes') for `readChunkSizeBytes: 0` (#119).
- `testAllowsDisabledHeartbeatAndEmptyServers` - Asserts that `pingIntervalSeconds: 0`, `maxPingsOut: 0`, and an empty servers list are all accepted (heartbeat-disabled is legitimate), preserving the zero values.
- `testDefaultsMatchDocumentedValues` - Asserts the full set of constructor defaults (servers, name, inboxPrefix, all timeouts, reconnect params, ping/TLS/auth fields, slowConsumerPolicy=DropOldest, reconnectBufferSize=8388608, WebSocket/logger defaults, readChunkSizeBytes=131072, etc.) match the README configuration table, field by field.

### tests/Unit/NkeySeedSignerTest.php
- `testPublicKeyMatchesKnownUserSeed` - Asserts `publicKey()` for a known sample user seed equals the expected public key; skips without the sodium extension.
- `testSignProducesVerifiableBase64UrlSignature` - Asserts `sign(nonce)` returns a base64url string whose decoded raw signature verifies against the seed's public key via `sodium_crypto_sign_verify_detached`; skips without sodium.
- `testInvalidSeedChecksumIsRejected` - Asserts constructing with a seed whose last character is corrupted throws NatsException ('checksum'); skips without sodium.
- `testTooShortBase32EncodingThrowsInvalidNKeyEncoding` - Asserts a base32 string ('AAAAA') decoding to only 3 bytes throws NatsException ('Invalid NKey encoding'); skips without sodium.
- `testNonZeroTrailingBitsThrowsInvalidTrailingBits` - Asserts a single base32 char ('B') with non-zero trailing bits throws NatsException ('Invalid trailing bits in NKey encoding'); skips without sodium.
- `testInvalidBase32CharacterThrowsException` - Asserts a seed containing '1' (outside the A-Z2-7 alphabet) throws NatsException ('Invalid base32 character in NKey encoding'); skips without sodium.
- `testSeedTooShortForDecodeSeedThrowsInvalidNKeySeedEncoding` - Asserts a CRC-valid but too-short seed ('KNKUCMNO', 3-byte payload below the 34-byte minimum) throws NatsException ('Invalid NKey seed encoding'); skips without sodium.
- `testWrongSeedPrefixB1ThrowsInvalidNKeySeedPrefix` - Asserts a 36-byte seed whose first byte b1 is 0 (not PREFIX_SEED=144) throws NatsException ('Invalid NKey seed prefix'); skips without sodium.
- `testInvalidPublicPrefixInSeedThrowsInvalidNKeySeedPrefix` - Asserts a seed with valid b1=144 but invalid public prefix byte b2=255 throws NatsException ('Invalid NKey seed prefix') via isValidPublicPrefix; skips without sodium.
- `testSeedInnerPayloadWrongLengthThrowsInvalidNKeySeedLength` - Asserts a seed with valid prefixes (b1=144, b2=160 USER) but a 33-byte inner seed (not 32) throws NatsException ('Invalid NKey seed length'); skips without sodium.
- `testSyntheticUserSeedIsAccepted` - Asserts a synthetic user seed (all-0x01 entropy) is accepted, its public key starts with 'U' and matches the base32 alphabet, and `sign()` returns a base64url signature; skips without sodium.
- `testSyntheticAccountSeedIsAccepted` - Asserts a synthetic account seed is accepted and produces a public key starting with 'A' matching the base32 alphabet; skips without sodium.

### tests/Unit/ObjectStoreBucketTest.php
- `testBucketCreateAndDelete` - create() maps the bucket to a stream with chunk+meta subjects and allow_rollup_hdrs, returning name OBJ_assets; deleteBucket() returns true and issues STREAM.CREATE/DELETE for OBJ_assets.
- `testPutUsesEncodedMetaSubjectAndNuidChunks` - put() returns ObjectInfo (name/size 5/chunks 1/non-empty nuid/correct digest), publishes chunks under the NUID chunk subject and rollup meta (Nats-Rollup:sub) under the base64url-encoded name subject.
- `testPutOmitsEmptyMetadataSoOfficialClientsCanRead` - put() with default metadata omits the metadata field entirely (no "metadata":[] array and no "metadata": key) for interop with official clients (#109).
- `testPutSerializesNonEmptyMetadataAsJsonObject` - put() with a populated metadata map serializes it as a JSON object ("metadata":{"team":"brand"}) (#109).
- `testPutStreamReChunksAndComputesDigestIncrementally` - putStream() with 3-byte chunkSize re-chunks a single 'hello' producer block into 2 chunks, reports size 5/chunks 2/correct digest, and emits exactly two chunk PUBs plus the meta HPUB.
- `testPutStreamReChunksLargeBlockAcrossManyChunks` - putStream() with 2-byte chunkSize splits one 'abcdef' producer block into 3 ordered chunks, reporting size 6/chunks 3/correct digest and three chunk PUBs.
- `testConstructorRejectsNonPositiveChunkSize` - constructing ObjectStoreBucket with chunkSize 0 throws JetStreamException mentioning "chunk size".
- `testPutStoresEmptyObjectWithZeroChunks` - put() of '' stores size 0/chunks 0, publishes no chunk message and only the meta HPUB under the encoded name subject.
- `testPutOverwritePurgesPreviousChunks` - put() over an existing object purges the previous revision's chunk subject via STREAM.PURGE referencing the old NUID.
- `testGetReturnsPayloadAndVerifiesDigest` - get() of a single-chunk object returns ObjectData with correct data/name/nuid, fetching the chunk via Direct Get (last_by_subj on the NUID subject) without creating an ephemeral consumer.
- `testGetVerifiesUnpaddedBase64UrlDigest` - get() verifies an UNPADDED base64url digest (as some non-Go clients store) against the padded computation and returns the data without a spurious mismatch.
- `testGetThrowsOnDigestMismatch` - get() throws JetStreamException "Object digest mismatch" when the downloaded chunk body does not match the metadata digest.
- `testGetToCallbackInvokesCallbackOnceForSingleChunkObject` - getToCallback() on a single-chunk object invokes the callback exactly once with that chunk and returns the ObjectInfo.
- `testGetToCallbackInvokesCallbackOncePerChunk` - getToCallback() on a multi-chunk object invokes the callback once per stored chunk in order (never assembling the whole object) and returns info with chunks=3.
- `testGetToCallbackReturnsNullForDeletedObjects` - getToCallback() on a tombstoned object returns null and never invokes the callback.
- `testDeleteWritesTombstoneAndPurgesChunks` - delete() returns info.deleted=true, writes a tombstone HPUB under the encoded meta subject and purges the previous revision's chunks via STREAM.PURGE on the old NUID.
- `testPutWithDescription` - put() with a description stores it on ObjectInfo.description and serializes "description":"A friendly doc" (#58).
- `testGetFollowsObjectLink` - get() of a link object transparently resolves the link to the target (doc.txt) and returns the target's content (#59).
- `testCreateWithTypedConfig` - create() with a typed ObjectStoreConfig maps ttl/maxBytes/storage/replicas/compression to STREAM.CREATE fields (max_age in ns, max_bytes, storage, num_replicas, compression) (#39).
- `testSeal` - seal() reads stream config then issues STREAM.UPDATE with sealed:true while preserving existing config (max_bytes:1000) (#38).
- `testAddLink` - addLink() returns a link ObjectInfo (isLink true, link={bucket,name}) and writes the link meta HPUB under the encoded name subject with "link":{"bucket":"assets","name":"real.bin"} (#48).
- `testAddLinkRejectsExistingLiveObject` - addLink() refuses to overwrite a LIVE object, throwing JetStreamException ('already exists', nats.go ErrObjectAlreadyExists parity) and writing no meta record, so the object's content stays reachable and its chunks are never orphaned.
- `testAddLinkAllowsRepointingAnExistingLink` - addLink() is allowed to re-point a name that currently holds a LINK (a link stores no chunks, so nothing is orphaned), returning a link ObjectInfo aimed at the new target.
- `testAddLinkRejectsDeletedTombstoneName` - addLink() over a name holding a DELETED tombstone is rejected with an already-exists error naming the object (nats.go reads the info with ShowDeleted) and writes no link meta record.
- `testAddLinkRejectsDeletedTargetAndLinkTarget` - addLink() rejects a deleted target ('deleted') and a target that is itself a link ('itself a link'), both of which would dangle by construction.
- `testMetaPublishNormalizesNoRespondersTo503` - A no-responders reply to the meta-record rollup publish (publishMeta, reached by every put/link/updateMeta) surfaces as JetStreamException(503) ("No JetStream responder"), not a bare NatsException, matching jsRequest()'s taxonomy (#161).
- `testAddBucketLink` - addBucketLink() returns a bucket-link ObjectInfo (link={bucket:'other-bucket'}) and serializes "link":{"bucket":"other-bucket"} (#48).
- `testUpdateMetaRenamesPreservingNuid` - updateMeta() rename preserves the NUID/size, writes new meta under the new encoded subject, tombstones the old name (deleted:true), and does NOT purge chunks (#28).
- `testUpdateMetaReplacesMetadataInPlace` - updateMeta() with no rename replaces the metadata bag in place (new map returned) writing only one meta HPUB and no tombstone (#28).
- `testListEnumeratesMetaSubjects` - list() on a pre-2.11 server paginates meta subjects via subjects_filter then Direct Gets each record with the per-subject fan-out fallback, excluding deleted by default (1 active) and including them with includeDeleted:true (2 total) (#110 gate routes 2.10 here).
- `testListPaginatesAcrossSubjectPages` - list() (pre-2.11 fan-out) collects objects across multiple STREAM.INFO subject pages (offsets 0,1,2) returning both a.txt and b.txt rather than truncating to the first page.
- `testListUsesSingleBatchedDirectGetOnModernServer` - list() on a 2.11+ server issues exactly ONE batched multi_last Direct Get for all objects (asserted count 1), returns BOTH active objects with the correct revisions (list not truncated to one); the single-request assertion fails on the pre-#110 per-subject fan-out (#110).
- `testInfoIncludesRevisionFromSequenceHeader` - info() sets ObjectInfo.revision from the Direct Get Nats-Sequence header (revision=2).
- `testInfoFallsBackToStreamMessageWhenDirectGetUnavailable` - info() falls back to STREAM.MSG.GET when Direct Get returns 503 and still returns the metadata, exercising both API subjects.
- `testListFallsBackToStreamMessageWhenDirectGetUnavailable` - list()'s per-subject fan-out falls back to the leader STREAM.MSG.GET per object when a bucket without Direct Get answers 503, returning the object instead of failing the whole enumeration.
- `testListFallbackQueriesLeaderByEnumeratedSubjectForNonCanonicalToken` - list()'s 503 fallback reads the leader by the ENUMERATED meta subject verbatim ("$O.assets.M.QQ", never a padded re-encode), so a record stored under a non-canonical base64url token still appears in the listing with its revision.
- `testInfoReturnsNullWhenNotFound` - info() returns null when Direct Get responds 404.
- `testWatchDispatchesObjectInfo` - watch() creates a push consumer (deliver_policy:new, ack_policy:none), and on a delivered meta frame dispatches an ObjectInfo with name/nuid and revision taken from the $JS.ACK stream sequence (7).
- `testWatchRequestsDefaultIdleHeartbeat` - watch() without options requests the default idle heartbeat (ObjectStoreBucket::WATCH_IDLE_HEARTBEAT_NS) on the consumer so the missed-heartbeat watchdog arms instead of a lost watch hanging silently (#113).
- `testWatchSilentConsumerIsRecreatedByWatchdog` - A silent Object Store watch consumer is deleted and recreated by the ordered-consumer watchdog at the ObjectStoreWatchOptions idleHeartbeat interval, re-applying the watch's initial deliver policy (never a by_start_sequence replay) and reporting no error (#113).
- `testWatchEncodesExactNamePatternAndRejectsWildcards` - watch() base64url-encodes an exact-name pattern into the consumer filter subject and rejects a wildcard pattern with the verbatim explanatory JetStreamException, rather than subscribing successfully and observing nothing.
- `testWatchExactNameEncodesNameContainingWildcardChars` - watch(exactName: true) treats a name containing '>' as a name and encodes it into "filter_subject", while the default still rejects it with a message naming the exactName escape hatch and creates no consumer.
- `testWatchWithOptionsRequestsSnapshotDeliverPolicy` - watch() given ObjectStoreWatchOptions requests deliver_policy:last_per_subject (snapshot-then-follow) instead of new (#98).
- `testWatchToleratesMalformedMetadataAndKeepsDispatching` - watch() silently skips a non-JSON meta delivery and still delivers a subsequent valid one (only the valid nuid is seen).
- `testWatchSkipsDeleteMarker` - watch() skips a server delete-marker frame (Nats-Marker-Reason) even with a valid non-empty body, while still delivering a later valid update (issue #5).
- `testInfoReturnsNullForDeleteMarker` - info() returns null when the latest meta record is a server delete-marker (Nats-Marker-Reason header) despite a non-empty body (issue #5).
- `testBucketSubjectHelpers` - streamName()/chunkPrefix()/metaPrefix() return OBJ_assets, $O.assets.C. and $O.assets.M. respectively.
- `testPutRejectsEmptyName` - put() with an empty object name throws JetStreamException "Invalid object name".
- `testPutSplitsIntoMultipleChunksWithSmallChunkSize` - put() of a 10-byte payload with chunkSize 4 splits into 3 chunks, emitting 3 chunk PUBs and recording "chunks":3.
- `testPutAbortsAndPurgesWhenChunkAcksArriveReordered` - put() aborts with a 'reordered' JetStreamException when the acked stream sequences are not increasing in chunk order, purging the partial NUID and publishing no meta record for the corrupt upload.
- `testPutDetectsReorderEvenWhenAcksArriveInMonotonicCompletionOrder` - put() collects ack sequences in CHUNK order, so a retried chunk whose ack arrives last with the highest sequence (monotonic arrival order) is still detected as reordered, aborting the upload and purging the partial NUID.
- `testPutOrderCheckStillDetectsInversionAfterSeqlessAck` - put()'s order check skips only the unverifiable pair around a seq-less ack (seq 0) and still aborts on a later inversion ('ack sequence 3 after 5'), purging the partial NUID and publishing no meta.
- `testPutFailurePurgesPartialChunksBeforeRethrowing` - put() purges the fresh NUID's already-stored chunks before rethrowing the ORIGINAL chunk-publish failure ('maximum bytes exceeded'), so no chunks are orphaned under a NUID no meta record references (nats.go purgePartial parity).
- `testGetDownloadsMultipleChunksInSingleBatch` - get() of a multi-chunk object uses a single batched pull (one CONSUMER.MSG.NEXT with batch:3 on the NUID filter_subject), reassembles in order and verifies the digest.
- `testListRethrowsNonNotFoundError` - list() rethrows a non-404 (500 'boom') error raised while Direct-Getting a meta record.
- `testListThrowsWhenSubjectEnumerationFails` - list() surfaces an error ('info failed') from the meta-subject enumeration (STREAM.INFO) request.
- `testDeleteToleratesPurgeFailure` - delete() still returns deleted=true when the best-effort chunk purge fails with a 500.
- `testListSkipsNotFoundSubject` - list() skips a meta subject whose Direct Get returns 404 and includes the remaining present object.
- `testDeleteToleratesMissingPreviousMetadata` - delete() proceeds (deleted=true) when the previous-metadata lookup fails with a non-404 (500) error.
- `testDeleteThrowsWhenMetadataPublishFails` - delete() throws JetStreamException 'publish rejected' when the tombstone metadata publish is rejected.
- `testGetStopsOnPullTimeoutAndFailsCompleteness` - get() that receives a 408 pull timeout mid-download fails the completeness gate with "Incomplete object download" rather than returning a truncated object.
- `testGetFailsTruncatedDownloadEvenWithoutDigest` - get() of an object with no digest still rejects a truncated download (chunk-count gate) with "Incomplete object download".
- `testGetRethrowsNonTimeoutPullError` - get() propagates a non-timeout pull error (409 Consumer Deleted) as a JetStreamException mentioning 'status 409'.
- `testGetReturnsNullWhenObjectNotFound` - get() returns null when info()'s Direct Get returns 404.
- `testGetReturnsNullForDeletedObject` - get() returns null for a deleted (tombstoned) object.
- `testGetThrowsOnTooManyLinkHops` - get() following a self-referential link chain throws "Too many Object Store link hops" once depth exceeds MAX_LINK_HOPS (8).
- `testGetThrowsOnBucketLink` - get() of a bucket link (link with no 'name') throws "it points to a bucket, not an object".
- `testGetToCallbackThrowsOnTooManyLinkHops` - getToCallback() following a self-referential link chain throws "Too many Object Store link hops".
- `testGetToCallbackReturnsNullWhenNotFound` - getToCallback() returns null and never calls the callback when the object's Direct Get returns 404.
- `testGetToCallbackFollowsObjectLink` - getToCallback() resolves an object link and streams the target's content to the callback, returning the target's info.
- `testGetSingleChunkThrowsIncompleteDownloadOnNotFound` - get() single-chunk path throws "Incomplete object download: expected 1 chunks, received 0" when the chunk Direct Get returns 404.
- `testGetSingleChunkRethrowsNonNotFoundNonUnavailableError` - get() single-chunk path rethrows a non-404/non-503 (500 'Stream Error Occurred') chunk Direct Get error.
- `testGetSingleChunkFallsThrough503ToEphemeralConsumer` - get() single-chunk path, on a 503 chunk Direct Get, falls through to the ephemeral consumer pull and successfully returns the chunk.
- `testGetSucceedsWhenStoredDigestIsEmpty` - get() succeeds without a digest check (returns the data) when the stored digest is empty.
- `testGetThrowsOnUnknownDigestPrefix` - get() throws "Object digest mismatch" when the stored digest has an unrecognised (non-SHA-256=) prefix that decodeDigest cannot parse.
- `testInfoRethrowsNonNotFoundNonUnavailableError` - info() rethrows a non-404/non-503 (500 'Downstream Error') Direct Get error.
- `testInfoReturnsNullWhenPayloadIsNotJson` - info() returns null when the Direct Get reply body is not valid JSON.
- `testInfoFallbackRethrowsNon404Error` - info() 503-fallback to STREAM.MSG.GET rethrows a non-404 (500 'stream error') error.
- `testInfoFallbackReturnsNullWhenMessageDataIsEmpty` - info() 503-fallback returns null when the STREAM.MSG.GET message has no 'data' field.
- `testUpdateMetaThrowsWhenObjectIsDeleted` - updateMeta() throws "Object not found: gone.txt" when the source object is a deleted tombstone.
- `testUpdateMetaThrowsWhenObjectNotFound` - updateMeta() throws "Object not found: missing.txt" when info() returns 404/null.
- `testUpdateMetaThrowsWhenRenameTargetExists` - updateMeta() rename throws "Cannot rename to an existing object: brand.txt" when the target exists and is not deleted.
- `testListReturnsEmptyArrayWhenBucketIsEmpty` - list() returns [] when STREAM.INFO reports an empty subjects map.
- `testListSkipsSubjectWithNonJsonBody` - list() skips a meta subject whose Direct Get body is non-JSON and returns only the valid object (good.txt).
- `testGetStatusReturnsMappedStreamState` - getStatus() maps stream state to bucket/stream/messages/last_sequence/bytes/subjects fields correctly.
- `testGetStatusDefaultsWhenStateIsAbsent` - getStatus() defaults messages/last_sequence/bytes to 0 and subjects to [] when the 'state' key is missing.
- `testPutStreamSkipsEmptyBlocks` - putStream() skips empty-string producer blocks (no hashing/buffering), processing only the non-empty 'hello' block (size 5/chunks 1/correct digest).
- `testPutStreamPurgesPreviousChunks` - putStream() purges the previous revision's chunks (STREAM.PURGE on the old NUID) when a prior object exists.
- `testInfoFallbackReturnsNullWhenMessageKeyAbsent` - info() 503-fallback returns null when the STREAM.MSG.GET response has no 'message' key.
- `testInfoFallbackReturnsNullWhenDataIsInvalidBase64` - info() 503-fallback returns null when the message 'data' field is not valid base64.
- `testPutOverwriteSwallowsPurgeFailure` - put() overwrite swallows a 500 purge failure and still returns the stored ObjectInfo without throwing.
- `testUpdateMetaSucceedsWhenRenameTargetIsDeleted` - updateMeta() rename succeeds when the target name exists but is a deleted tombstone (not a live conflict), preserving the source NUID.
- `testGetThrowsOnBucketLinkWithEmptyName` - get() treats a link with an empty 'name' as a bucket link and throws "it points to a bucket, not an object".
- `testGetToCallbackSingleChunkFallsThrough503` - getToCallback() single-chunk path falls through a 503 chunk Direct Get to the ephemeral consumer and delivers the chunk to the callback.
- `testListReturnsEmptyWhenStateKeyAbsent` - list() returns [] when the STREAM.INFO response has no 'state'/'subjects' at all.
- `testPutStreamFailurePurgesPartialChunksBeforeRethrowing` - putStream() abandons a streamed upload whose chunk publish fails: the partial NUID's chunks are purged, the ORIGINAL failure ('maximum bytes exceeded') is rethrown and no meta record is published.
- `testListBatchedRethrowsNonUnavailableError` - list()'s batched fast path propagates a non-503 batched Direct Get error verbatim (code 500, 'direct get batch exploded') instead of silently retrying through the per-subject fan-out or the leader read.
- `testPutSucceedsWhenServerReportsNoAckSequences` - put() succeeds normally when the server acks chunks without a sequence, so seq-less acks never false-positive as a reordered upload: the meta record is published and no purge is issued.
- `testWatchOptionsRejectNonPositiveIdleHeartbeat` - ObjectStoreWatchOptions rejects a zero or negative idleHeartbeat at construction with InvalidArgumentException ('idleHeartbeat must be a positive integer (nanoseconds)'), matching the KV watch options (#113).
- `testAddLinkPropagatesNameLookupErrorAndPublishesNothing` - addLink() propagates a failed name-free lookup (500 'name lookup failed') rather than reading it as "name free", running no target lookup and publishing no link meta.
- `testPutStopsPublishingChunksWhenAWindowFlushFails` - put() bounds in-flight chunks to UPLOAD_PIPELINE_DEPTH (16): when a full window's flush fails, exactly 16 chunk PUBs reach the wire (the 17th is gated), the partial NUID is purged and no meta record is published.
- `testPutStreamStopsPublishingChunksWhenAWindowFlushFails` - putStream() applies the same pipeline-window bound, stopping after 16 chunk PUBs when the full window's flush fails, purging the partial NUID and publishing no meta record.
- `testPutStreamAbortsAndPurgesWhenChunkAcksArriveReordered` - putStream() runs the same upload-order guard as put(), aborting with the verbatim reorder diagnostic ('ack sequence 1 after 2'), purging the partial NUID and publishing no meta record.
- `testPutAbortsWhenAckSequenceRepeats` - put()'s order guard is STRICTLY increasing: a repeated ack sequence ('ack sequence 5 after 5') aborts the upload and purges the partial NUID exactly like an inversion.
- `testAbandonedUploadDrainsInFlightRetryBeforePurging` - abandonPartialUpload() drains still-in-flight chunk publishes before purging, so a delayed 503 retry's chunk PUB lands before the purge request (three chunk PUBs precede it) and cannot re-orphan its chunk, while the original failure still surfaces.
- `testListBatched503FallsBackToPerSubjectFanOut` - list()'s batched fast path falls through to the per-subject Direct Get fan-out on a 503 (the stream lacks Direct Get), returning the object via a per-subject last_by_subj read instead of failing the enumeration.

### tests/Unit/ObjectStoreConfigTest.php
- `testToStreamConfigIncludesDescription` - Asserts toStreamConfig() includes the 'description' key when description is set on the ObjectStoreConfig.
- `testToStreamConfigIncludesPlacement` - Asserts toStreamConfig() passes a set placement array through unchanged under the 'placement' key.
- `testToStreamConfigMapsAllFields` - Asserts a fully-populated ObjectStoreConfig maps every field, including ttlSeconds->max_age in ns (3600->3600e9), max_bytes, storage, num_replicas, compression, description, and placement.
- `testToStreamConfigReturnsEmptyArrayForDefaultInstance` - Asserts a default ObjectStoreConfig (no fields set) produces an empty array from toStreamConfig().

### tests/Unit/ObjectStoreWatchOptionsTest.php
- `testDefaultOptionsReplayCurrentStateThenFollow` - Asserts default watch options map to deliver_policy 'last_per_subject' (snapshot-then-follow) and ack_policy 'none'.
- `testUpdatesOnlyRequestsNewDeliverPolicy` - Asserts updatesOnly:true maps to deliver_policy 'new'.
- `testIncludeHistoryRequestsAllDeliverPolicy` - Asserts includeHistory:true maps to deliver_policy 'all'.
- `testIncludeHistoryTakesPrecedenceOverUpdatesOnly` - Asserts when both updatesOnly and includeHistory are true, includeHistory wins and deliver_policy is 'all'.

### tests/Unit/ProtocolCodecTest.php
- `testEncodeConnectContainsName` - asserts encodeConnect output starts with `CONNECT ` and contains the configured `"name":"test-client"` field.
- `testEncodeHeaderPublishBlockMatchesEncodeHeaderPublish` - asserts the precomputed-block variant `encodeHeaderPublishBlock` produces byte-identical frames to `encodeHeaderPublish` both with and without a replyTo.
- `testEncodeConnectAdvertisesResolvedClientVersion` - asserts CONNECT carries a `"version":"..."` resolved from the installed package and no longer the hardcoded `0.1.0-dev` literal.
- `testEncodeConnectContainsPasswordAuthFields` - asserts CONNECT includes `"user":"alice"` and `"pass":"s3cr3t"` for username/password auth.
- `testEncodeConnectContainsTokenAuthField` - asserts CONNECT includes `"auth_token":"token-123"` for token auth.
- `testEncodeConnectUsesTokenProviderPerConnect` - asserts the tokenProvider callback is invoked per encode (rotated-token-1, then -2), overrides the static token, and is called exactly twice.
- `testEncodeConnectUsesUrlUserPassword` - asserts URL-embedded user/pass (`url-user`/`url-pass`) are used and override the static options' credentials.
- `testEncodeConnectUsesUrlToken` - asserts a URL-embedded token is emitted as `"auth_token":"url-token"`.
- `testEncodeConnectUsesJwtProviderPerConnect` - asserts the jwtProvider callback supplies a fresh JWT per encode, overrides the static jwt, and the nonce is signed (`"sig":"sig:nonce-1"`).
- `testEncodeConnectContainsJwtAuthFields` - asserts CONNECT includes `"jwt"`, `"nkey"`, and a server-nonce-signed `"sig"` for JWT auth.
- `testEncodeConnectJwtRequiresSignerAndNonce` - asserts JWT auth without a nonce signer throws ProtocolException.
- `testEncodePublishWithoutReply` - asserts encodePublish produces `PUB orders.created 3\r\nabc\r\n` using payload length and CRLF framing.
- `testParseServerInfo` - asserts parseServerInfo maps serverId, serverName, jetStreamEnabled, and maxPayload from an INFO payload.
- `testEncodeHeaderPublish` - asserts HPUB output starts with `HPUB orders.created ` and embeds the `NATS/1.0` header block, header line, and CRLF-framed payload.
- `testEncodeConnectContainsNoEchoFalse` - asserts CONNECT includes `"echo":false` when noEcho is enabled.
- `testEncodeConnectDefaultEchoTrue` - asserts CONNECT includes `"echo":true` by default when noEcho is disabled.
- `testEncodeConnectStandaloneNkeyAuth` - asserts standalone NKey auth emits `"nkey"` and signed `"sig"` but no `"jwt"` field.
- `testEncodeConnectNkeyRequiresSigner` - asserts standalone NKey auth without a nonce signer throws ProtocolException with the "requires a nonce signer" message.
- `testEncodeConnectNkeyRequiresServerNonce` - asserts standalone NKey auth without a server nonce throws ProtocolException with the "requires server nonce from INFO" message.
- `testEncodeConnectJwtWithSignerButNullNonceThrows` - asserts JWT auth with a present signer but null nonce throws ProtocolException ("requires server nonce from INFO").
- `testEncodeConnectJwtWithSignerButEmptyNonceThrows` - asserts JWT auth with a present signer but empty-string nonce throws the same server-nonce ProtocolException.
- `testEncodeConnectNkeyMismatchWithSeedSignerThrows` - asserts a configured nkey that does not match the public key derived from the NkeySeedSigner seed throws ProtocolException (skips without the sodium extension).
- `testDecodeInfoLineThrowsOnNonInfoPrefix` - asserts decodeInfoLine throws ProtocolException ("Expected INFO line from server") for a non-INFO line like `PING`.
- `testDecodeInfoLineThrowsOnBareInfoWithoutSpace` - asserts a bare `INFO` line without a trailing space throws the "Expected INFO line" prefix error.
- `testDecodeInfoLineReturnsCommandAndPayload` - asserts decodeInfoLine returns command `INFO` and the raw JSON payload on the happy path.
- `testDecodeInfoLineTrimsWhitespace` - asserts decodeInfoLine trims surrounding CRLF before checking the prefix and still returns command and payload.
- `testEncodeConnectFallsBackToConstantVersionWhenPackageMetadataIsUnavailable` - asserts that when Composer runtime metadata cannot report this package's version (datasets blanked via reflection to simulate a from-source or vendored checkout), CONNECT still advertises the non-empty `FALLBACK_CLIENT_VERSION` constant rather than failing the handshake or sending an empty version.

### tests/Unit/ProtocolParserFuzzTest.php
- `testArbitraryBytesOnlyRaiseProtocolException` - asserts 3000 seeded random byte chunks (0-80 bytes) only ever raise ProtocolException (never another Throwable) and the parser never hangs, with all iterations counted.
- `testRandomProtocolTokenSoupOnlyRaisesProtocolException` - asserts 3000 seeded chunks built from real protocol tokens with adversarial spacing/truncation (plus an empty follow-up push) only raise ProtocolException, all iterations counted.
- `testByteAtATimeReassemblyMatchesSinglePush` - asserts a multi-frame stream (PING/MSG/PONG/HMSG-with-reply/+OK/INFO/empty-payload MSG) fed one byte at a time produces frames identical (type, subject, sid, replyTo, payload, headerBytes, totalBytes, infoPayload) to a single push.
- `testUnterminatedControlLineIsBoundedNotUnbounded` - asserts a >1 MiB control line with no CRLF throws ProtocolException rather than buffering unboundedly.
- `testOversizedDeclaredMsgPayloadIsRejected` - asserts an MSG declaring a 1000-byte payload against a 64-byte bound is rejected with ProtocolException at header-parse time.
- `testParserResyncsAfterMalformedControlLine` - asserts after a malformed `GARBAGE-NOT-A-VERB` line throws, a subsequent `PING\r\n` still parses, proving the buffer resynced past the bad line.

### tests/Unit/ProtocolParserTest.php
- `testRejectsOverflowingSizeField` - asserts a 20-digit MSG size field (exceeding PHP_INT_MAX) is rejected with ProtocolException instead of saturating.
- `testParsesControlFrames` - asserts a stream of PING/PONG/+OK/-ERR parses into four frames with correct types and the -ERR error string `'boom'`.
- `testParsesControlLinesWithMultiSpaceAndEmbeddedTabSeparators` - asserts the whitespace-tolerant control-line split (pinned across the #140 explode fast path and its #163 preg_split revert): multi-space-separated MSG/HMSG lines tokenize, and a tab hidden inside a space-separated token (making a 4-token-looking MSG line really the 5-token reply-to form) still yields the reply-to.
- `testControlLineTokenizationTreatsAnyWhitespaceRunAsSeparator` - asserts the control-line tokenizer treats ANY whitespace run as a separator (guarding the #163 revert of splitControlLine to preg_split): a vertical tab or form feed embedded in an otherwise space-separated MSG/HMSG line still delimits the real fields, a mixed space+tab run collapses to one separator, and tab-only HMSG reply-to lines parse - cases a naive explode(' ') would miscount.
- `testParsesFragmentedMsgFrame` - asserts an MSG split across two pushes yields no frame on the first and a complete frame (subject, sid 17, payload `hello`) on the second.
- `testParsesHmsgFrame` - asserts an HMSG (no replyTo) parses with subject, sid, headerBytes 12, totalBytes 17, and the combined header+payload bytes.
- `testParsesHmsgFrameWithReplyTo` - asserts an HMSG with a reply subject parses replyTo `inbox.reply` along with subject, sid, byte counts, and payload.
- `testBuffersPartialControlLineUntilCrLf` - asserts `PIN` then `G\r\n` buffers until CRLF and then yields a single Ping frame.
- `testThrowsForUnsupportedFrame` - asserts an unsupported command (`WAT ...`) throws ProtocolException.
- `malformedMessageLineProvider` - data provider yielding malformed MSG/HMSG control lines (too few/too many fields).
- `testRejectsMalformedMessageLines` - asserts each malformed MSG/HMSG line from the provider throws ProtocolException.
- `testRejectsMessagePayloadWithoutTerminatingCrLf` - asserts an MSG whose payload is not terminated by the expected CRLF throws ProtocolException.
- `testPropertyStyleFragmentedMsgReassembly` - asserts an MSG wire reassembles to one correct frame across many deterministic fragmentation patterns.
- `testPropertyStyleFragmentedHmsgReassembly` - asserts an HMSG wire (with a Status header) reassembles to one correct frame across many fragmentation patterns.
- `testParsesLargeFragmentedMsgWithEmbeddedCrlf` - asserts a 6000-byte MSG payload containing embedded CRLF bytes reassembles correctly when fed one byte at a time.
- `testParsesLargePayloadDeliveredInTransportSizedChunks` - asserts a 300000-byte MSG payload (embedded CRLFs) delivered in 8 KiB chunks reassembles byte-identically via the #140 pending chunk-list accumulation, a mid-accumulation empty push emits nothing, and a PING arriving in the completing chunk parses as the next frame.
- `testCompletedPendingFrameLeavesTrailingBytesForNextFrame` - asserts a pending MSG payload completes and a trailing PONG in the same push parses as a second frame.
- `testRejectsMsgFrameExceedingMaxSize` - asserts a MSG payload exceeding the configured maxFrameSize throws ProtocolException ("MSG frame payload size is invalid").
- `testRejectsHmsgFrameExceedingMaxSize` - asserts an HMSG payload exceeding the configured maxFrameSize throws ProtocolException ("HMSG frame payload size is invalid").
- `testDefaultMaxFrameSizeAllowsLargePayloads` - asserts the default (8 MiB) parser accepts a 1024-byte MSG payload, yielding one frame of length 1024.
- `testRejectsNonNumericMsgSid` - asserts a non-numeric MSG sid (`xyz`) throws ProtocolException ("Invalid sid") rather than coercing to 0.
- `testRejectsNonNumericMsgSize` - asserts a non-numeric MSG size (`abc`) throws ProtocolException ("Invalid payload size").
- `testRejectsNegativeMsgSize` - asserts a negative MSG size (`-5`) throws ProtocolException ("Invalid payload size").
- `testRejectsHmsgHeaderBytesExceedingTotal` - asserts an HMSG whose header bytes exceed total bytes throws ProtocolException ("header bytes exceed total bytes").
- `testBuffersSubCapControlLineWithoutCrlf` - asserts a 1000-byte partial control line with no CRLF is buffered (returns no frames), not rejected.
- `testRejectsUnterminatedControlLineExceedingBound` - asserts a control line over 1 MiB with no CRLF throws ProtocolException ("Control line exceeds maximum length") as an OOM guard.
- `testResyncsPastMalformedControlLineInsteadOfPoisoning` - asserts after a malformed `BADOP` line throws, the offending line is consumed so a subsequent `PING\r\n` parses normally.
- `testRetainsFramesParsedBeforeMidChunkFailureForCatchSiteDrain` - asserts a chunk of `[valid MSG][garbage line]` throws but retains the parsed MSG (subject/sid/payload intact) for `takeParsedFrames()`, and a second take returns empty (draining is destructive) (#147).
- `testUndrainedRetainedFramesArePrependedToNextPushResult` - asserts retained frames a catch site never drained are returned by the next push ahead of newly parsed frames (MSG then PING), preserving wire order (#147).
- `testTakeParsedFramesIsEmptyAfterSuccessfulPush` - asserts a clean push returns all frames via its normal return, leaving nothing in `takeParsedFrames()` to duplicate into a later drain.
- `testParsesVerbsCaseInsensitivelyAndWithTabSeparators` - Parses protocol verbs case-insensitively and accepts tab separators per the wire spec, while preserving the case and whitespace of arguments and payloads.

### tests/Unit/PullConsumerIteratorTest.php
- `testFluentBuilderSetsProperties` - verifies the fluent builder (`setBatching`/`setExpiresMs`/`setIterations`) returns a `PullConsumerIterator` and stores 10/5000/3 via the getters.
- `testDefaultValues` - asserts a freshly created pull consumer defaults to batching 1, expiresMs 3000, and null iterations (infinite).
- `testSetBatchingRejectsZero` - `setBatching(0)` throws `JetStreamException`.
- `testSetExpiresMsRejectsZero` - `setExpiresMs(0)` throws `JetStreamException`.
- `testSetIterationsRejectsZero` - `setIterations(0)` throws `JetStreamException`.
- `testSetIterationsAcceptsNull` - `setIterations(5)` then `setIterations(null)` leaves iterations null (clearable).
- `testHandleProcessesOneIteration` - with batching 1 and 2 iterations, one delivered message is processed then a terminal 404 breaks the loop; returns total 1 and payload `['order-1']`.
- `testStopAbandonsRestOfBatch` - calling `stop()` inside the handler ends the consume loop after the first message of a 3-message batch, abandoning the rest; total is 1 and only `['m1']` processed (#32).
- `testDrainFinishesBatchThenStops` - calling `drain()` inside the handler lets the in-flight batch of 3 finish but issues no further pull; total is 3 and all `['m1','m2','m3']` processed (#32).
- `testOnErrorFiresOnTerminalStatus` - a terminal 409 Consumer Deleted status surfaces a `JetStreamException` to the onError callback (code 409, message contains "Consumer Deleted"), delivers no message, and returns total 0 (#63).
- `testOnErrorNotFiredOnRoutineEmptyWindow` - a routine 404 No Messages status does not fire the onError callback (#63).
- `testHandleStopsOnNoMessages` - an immediate terminal 404 stops handle() without invoking the handler and returns total 0.
- `testHandleInfiniteModeContinuesPastEmptyWindow` - in infinite mode a 404 empty window does not stop the loop; polling continues, the next message is delivered (total 1, `['order-1']`), then a terminal 409 stops it.
- `testHandleInfiniteModeContinuesPastTransient409` - in infinite mode a transient 409 (Exceeded MaxAckPending) keeps polling; the subsequent message is delivered (total 1, `['job-7']`) and a terminal 409 Consumer Deleted stops the loop.
- `testHandleInfiniteModeContinuesPastMaxBytes409` - a 409 "Message Size Exceeds MaxBytes" is a pull-completion status, not terminal: an infinite loop with setMaxBytes() keeps pulling past it and delivers the next message (total 1, `['fits-now']`) instead of stopping the worker permanently (#153).
- `testHandleInfiniteModeContinuesPastBatchCompleted409` - a 409 "Batch Completed" is a pull-completion status, not terminal: the infinite loop re-pulls and delivers the next message (total 1, `['next-batch']`) (#153).
- `testNoWaitInfiniteModePacesConsecutiveEmptyPulls` - three consecutive immediate 404s on a no_wait infinite loop are paced by the escalating idle backoff (monotonic elapsed >= 65ms for the 10+20+40ms schedule; exactly one pull per scripted response, no storm), then the message is delivered and a terminal 409 stops the loop (#153).
- `testSetGroupRejectsInvalidName` - `setGroup()` with an over-long/invalid group name throws `JetStreamException`.
- `testSetGroupAcceptsNull` - `setGroup('g1')` then `setGroup(null)` clears the group without throwing and returns the iterator.
- `testSetPriorityRejectsNegative` - `setPriority(-1)` throws `JetStreamException`.
- `testSetPriorityRejectsAboveNine` - `setPriority(10)` throws `JetStreamException`.
- `testSetPriorityAcceptsValidValues` - `setPriority(5)` returns `$this`; boundary values 0 and 9 are accepted.
- `testSetPriorityAcceptsNull` - `setPriority(3)` then `setPriority(null)` clears the priority and returns the iterator.
- `testSetMinPendingStoresValue` - `setMinPending(42)` returns `$this`.
- `testSetMinPendingAcceptsNull` - `setMinPending(10)` then `setMinPending(null)` clears the threshold and returns the iterator.
- `testSetMinAckPendingStoresValue` - `setMinAckPending(7)` returns `$this`.
- `testSetMinAckPendingAcceptsNull` - `setMinAckPending(5)` then `setMinAckPending(null)` clears the threshold and returns the iterator.
- `testSetMaxBytesStoresValue` - `setMaxBytes(1024)` returns `$this`.
- `testSetMaxBytesAcceptsNull` - `setMaxBytes(512)` then `setMaxBytes(null)` clears the cap and returns the iterator.
- `testSetNoWaitStoresValue` - `setNoWait(true)` returns `$this`; `setNoWait(false)` clears the flag.
- `testSetNoWaitDefaultsToTrue` - `setNoWait()` with no argument returns `$this` (defaults to true).
- `testBuildPullIncludesAllOptionalFields` - with priority/minPending/minAckPending/maxBytes/noWait set, the issued CONSUMER.MSG.NEXT pull JSON contains `"priority":3`, `"min_pending":10`, `"min_ack_pending":5`, `"max_bytes":65536`, and `"no_wait":true`.
- `testBuildPullOmitsUnsetOptionalFields` - when those optional fields are unset, none of `"priority"`, `"min_pending"`, `"min_ack_pending"`, `"max_bytes"`, `"no_wait"` appear in the pull payload.
- `testReusedIteratorAfterStopStartsFresh` - an iterator that called `stop()` in run 1 (processing only `['m1']`) is not pre-stopped on a second `handle()`; the second run delivers `['fresh-msg']`, proving resetLifecycle clears the stop flag.
- `testReusedIteratorAfterDrainStartsFresh` - an iterator that called `drain()` in run 1 (`['run1']`) is not pre-drained on a second `handle()`; the second run polls again and delivers `['run2']`, proving resetLifecycle clears the drain flag.
- `testOnErrorNotFiredOnRoutine408` - a routine 408 Request Timeout status does not fire the onError callback.
- `testHandleRePinsOnStalePin` - a 423 Nats-Pin-Id Mismatch drops the pin id and re-pulls without it (#7); the new pin id from the next delivery is captured and resent on the following pull; asserts total 2, payloads `['order-9','order-10']`, and that pull 1 carries `"group":"g1"` with no `"id"`, pull 2 has no `"id"`, and pull 3 carries `"id":"pin-new"`.
- `testSetDepthRejectsZero` - setDepth(0) throws, so a pipeline depth that would issue no pulls cannot be configured.

### tests/Unit/PullPipelineTest.php
- `testSinglePullDeliversItsBatch` - asserts a single finite pull delivers its whole batch (total 2, payloads `['a','b']`) and that the pipelined engine uses one long-lived pull inbox: exactly one `SUB _INBOX.JS.PULL.` and one `UNSUB` at teardown, with no per-pull subscription churn (#120).
- `testOverlapIssuesSecondPullBeforeDrainingTheFirst` - verifies depth 2 pipelines even at batch 1: both pulls are on the wire before the first is drained (2 pull PUBs by the first delivery) and the run processes 2 messages before a terminal 409 stops it (#120).
- `testFiniteModeIssuesExactlyNPullsAndStopsOnFirstEmpty` - asserts finite mode (iterations 3) runs serially and stops at the first empty window: a delivering pull followed by a 404 ends the run at 2 pull PUBs instead of issuing all N.
- `testTerminalStatusStopsAndReportsOnErrorExactlyOnce` - asserts a terminal 409 Consumer Deleted stops the run with total 0 and surfaces exactly one `JetStreamException` to onError whose message names the status.
- `testInfiniteRunPollsThroughTransientNoResponders` - verifies a 503 No Responders is transient rather than terminal: the infinite run polls through it and delivers the next message (total 1, `['a']`), reporting the first 503 of the streak once (code 503) plus the later terminal 409, two onError reports in all.
- `testInfiniteRunWithoutOnErrorSurvivesTransientNoResponders` - asserts an infinite worker with no onError configured survives a momentary 503 and still processes the following message (returns 1), instead of terminating silently and resolving like a clean drain.
- `testNoRespondersSignalReArmsAfterRoutineNonEmptyGap` - asserts the one-shot no-responders signal re-arms on any routine non-503 retire (a 404 proves the JS API answers again), so a second outage on an idle stream that never delivers between outages is still reported: both 503s plus the terminal 409 reach onError.
- `testPermissionRejectedPullInboxFailsFastInsteadOfSpinning` - verifies a pull reply inbox rejected by server permissions fails the run promptly with a `JetStreamException` whose full message pins the rejected subject, the server error, the `_INBOX.JS.PULL.>` wildcard to grant and the remedy, rather than spinning forever with no signal on any channel (#167).
- `testStopAbandonsRemainingBatch` - asserts `stop()` inside the handler abandons the rest of the in-flight batch: only the first of 3 messages is processed (total 1, `['a']`).
- `testGroupedUnpinnedRunFansOutAfterFirstDelivery` - asserts a priority group that never receives a `Nats-Pin-Id` pulls serially only until its first bootstrap delivery and then fans out to `setDepth()`, putting 3 pull PUBs on the wire by the second delivery instead of staying clamped to serial (#120).
- `testPinnedGroupReserializesPinRecaptureAfterStalePin` - verifies a pinned_client group whose pin is cleared by a 423 re-serializes its pin re-capture, pulling one at a time (4 pull PUBs by the next delivery) instead of racing pin-less pulls at full depth again (#170).
- `testDeliveringPipelineIsNotClampedByInterleavedNoWaitEmpties` - asserts a lone no_wait 404 interleaved with deliveries does not latch the idle drain (streak below depth): the pipeline keeps refilling at full depth with no backoff pause and no serial clamp, reaching 6 pull PUBs (#169).
- `testProductiveThenIdleStreamStillBacksOff` - asserts the empty streak survives the engine's continuous refill, so a stream that delivers and then goes idle still latches the idle drain, paces its relaunch by the first 10ms backoff step, and clamps the next generation serial (exactly 5 pulls) (#169).
- `testDrainFinishesInFlightBatchThenStops` - asserts `drain()` inside the handler lets the whole in-flight batch of 3 finish (total 3, `['a','b','c']`) while issuing no further pull.
- `testStopLatchedOutsideRetirePhaseEndsRunAtNextLoopTurn` - verifies `stop()` latched while the engine is suspended outside the retire phase (inside a pull request's own publish) ends the run at the next loop-top stop check with no 4th pull issued, and still releases the long-lived pull inbox with exactly one `UNSUB`.
- `testIdleHeartbeatMissWakesTheEngineButIsNotTerminal` - asserts a route-wide idle-heartbeat miss is a wake-up and not a terminal error for the pipelined engine: the miss deadline bounds the first read segment (at least 2 started reads prove the early wake), the pull still waits out its own deadline, and the finite run ends with total 0 and no onError, unlike `fetchBatch()` which throws on the same silence.
- `testExpiredSiblingDeadlineDuringSlowHandlerRetiresWithoutBlocking` - asserts a sibling pull whose deadline expires while the handler is busy is retired on the next loop turn without building a read wait from a negative remainder and without blocking on a read first: the run keeps refilling (4 pull PUBs) and reports the genuine terminal 409 exactly once.
- `testCapturedPinPersistsIntoTheNextRun` - verifies resetLifecycle() keeps the captured pin between runs: run 1's bootstrap pull carries no `"id"`, and the first pull of the second `handle()` re-joins the pin with `"id":"p1"` instead of racing an unpinned bootstrap.
- `testFirstNoRespondersAfterADeliveryIsStillSignaled` - asserts a delivery re-arms the one-shot no-responders signal, so the first 503 of an outage that starts after a productive stretch is still reported (code 503) ahead of the terminal 409.
- `testConsecutiveNoRespondersSignalOnlyOnce` - asserts the no-responders signal is one-shot per outage episode: two consecutive 503s produce a single onError report and only the terminal 409 adds a second, so a downed JetStream is never reported once per poll.
- `testNoRespondersEmptiesPaceTheirRelaunches` - asserts a 503 empty retire warrants the escalating idle backoff: three consecutive 503 generations pace their relaunches by 10 + 20 + 40 ms (monotonic elapsed >= 60ms) before the terminal pull, instead of hot-polling a downed JetStream API (#153).
- `testDrainRetiresAllDueEmptiesInOneTurnWithoutDeadlineWait` - asserts the retire phase retires every due head pull in one turn: with depth 2 and both in-flight pulls answered 404 in a single read chunk under `drain()`, the run ends promptly (< 800ms) rather than sitting out the second pull's client-side deadline on a blocking read.

### tests/Unit/ReadPathProgressTest.php
- `testReadIncomingReportsConsumedBytesOnPartialFrameAndIdleOnEmptyRead` - Feeds a 50-byte MSG frame one byte at a time and asserts `readIncoming()` reports `consumedBytes=true` on every byte-consuming read even while the frame is incomplete (frames=0), exactly one read completes the frame (frames=1), a subsequent empty read reports `consumedBytes=false`, and the reassembled payload is byte-identical (#119). Falsifiable: a naive `consumedBytes = frames>0` would report false on the partial reads.
- `testProcessIncomingStillReturnsFrameCountForMultiChunkMessage` - Splits a 1600-byte message into 7-byte chunks and asserts the public `processIncoming()` still sums to exactly one frame across the partial reads and delivers the payload identically, pinning that the #119 refactor left the frame-count contract unchanged.
- `testChunkedMessageIsCollectedWithoutIdleSleepPerChunk` - Drives `SubscriptionQueue::fetchAll(1)` over a 317-chunk (one byte each) message with a 60 ms timeout window and asserts the message is collected inside the window with the correct payload (#119). Falsifiable: on the pre-fix path each of the ~316 partial reads sleeps 1 ms (~316 ms), overrunning the window so fetchAll returns [] - verified by temporarily restoring the unconditional sleep.
- `testIdleWaitRespectsDeadlineAndDoesNotBusySpin` - With a live-but-silent socket (blockWhenEmpty), asserts `next()` with a 50 ms timeout returns null at ~the deadline (>= 45 ms, does not bail early), terminates (does not hang), and bounds its read with a cancellation - the genuinely-idle path still yields and honors its deadline (#119).

### tests/Unit/RepublishAndTransformTest.php
- `testRepublishMinimal` - Asserts Republish::create(src, dest)->toArray() yields exactly ['src','dest'] with no headers_only key.
- `testRepublishHeadersOnly` - Asserts headersOnly() adds headers_only=true to the republish array.
- `testRepublishHeadersOnlyFalse` - Asserts calling headersOnly(true) then headersOnly(false) omits the headers_only key, leaving only src and dest.
- `testSubjectTransform` - Asserts SubjectTransform::create(src, dest)->toArray() yields exactly ['src','dest'].
- `testSubjectTransformWithTokenMapping` - Asserts token-mapping patterns (e.g. 'input.*.data' -> 'output.$1.processed') are preserved verbatim in the src/dest keys.

### tests/Unit/ScheduleTest.php
- `testAtFormatsUtcExpression` - `Schedule::at()` formats a UTC `DateTimeImmutable` as "@at 2030-01-01T00:00:00Z".
- `testAtNormalizesTimezoneToUtc` - `Schedule::at()` normalizes a Europe/Warsaw time to UTC, yielding "@at 2030-01-01T00:00:00Z".
- `testAtTimestamp` - `Schedule::atTimestamp(1893456000)` returns the equivalent "@at 2030-01-01T00:00:00Z" expression.
- `testEveryFromSeconds` - `Schedule::every(30)` produces "@every 30s".
- `testEveryFromDurationString` - `Schedule::every('1h30m')` produces "@every 1h30m".
- `testEveryRejectsNonPositiveSeconds` - `Schedule::every(0)` throws `InvalidArgumentException`.
- `testEveryRejectsEmptyString` - `Schedule::every('   ')` throws `InvalidArgumentException`.
- `testCronReturnsSixFieldExpression` - `Schedule::cron('0 0 0 * * *')` returns the valid 6-field expression unchanged.
- `testCronRejectsNonSixFieldExpression` - `Schedule::cron('0 0 * * *')` (5-field unix cron) throws `InvalidArgumentException`.
- `testPredefinedNormalizesAlias` - `Schedule::predefined()` normalizes aliases with/without leading "@" and any case ("daily"->"@daily", "@hourly"->"@hourly", "MONTHLY"->"@monthly").
- `testPredefinedRejectsUnknownAlias` - `Schedule::predefined('fortnightly')` throws `InvalidArgumentException` for an unknown alias.

### tests/Unit/ServiceTest.php

- `testDoneHandlerFiresOnceOnStop` - Asserts the onDone handler fires exactly once per stop (a second stop() does not re-fire), and that restarting re-arms it so a subsequent stop fires it again (#57).
- `testEndpointStatsHandlerMergesCustomData` - Asserts a per-endpoint stats supplier's custom data (e.g. `queue_depth` and the endpoint name) is merged into the endpoint's `data` key in the stats snapshot (#50).
- `testGroupedEndpointForwardsMetadataAndStatsHandler` - Asserts a grouped endpoint gets the group-prefixed subject (`v1.work`) and that its stats supplier output (`['ok' => true]`) appears as the endpoint `data` (#40).
- `testDrainUnsubscribesAndFlushes` - Asserts drain() emits UNSUB frames for endpoints/discovery and a PING to flush them over the wire (#51).
- `testStartRegistersSubscriptions` - Asserts start() writes discovery SUBs ($SRV.PING, $SRV.INFO.echo, $SRV.STATS.echo) and the endpoint SUB with its queue group (`svc.echo q.echo`).
- `testInfoIncludesEndpointMetadata` - Asserts the $SRV.INFO discovery response includes per-endpoint metadata (`"metadata":{"team":"core"}`) per the NATS micro spec.
- `testAddEndpointRejectsNonTokenName` - Asserts an endpoint name that is not a valid NATS token (`my endpoint!`) is rejected at registration with an InvalidArgumentException mentioning ADR-32, so an invalid name is never advertised verbatim through $SRV.INFO/$SRV.STATS.
- `testDiscoveryEncodesEmptyMetadataAsObject` - Asserts empty metadata maps serialize as JSON objects (`"metadata":{}`) and never as arrays (`[]`) in the PING, INFO, and STATS discovery responses, so ADR-32 conformant Go tooling can unmarshal them.
- `testInfoIncludesEndpointSchema` - Asserts a declared endpoint schema appears in the standard $SRV.INFO `io.nats.micro.v1.info_response` (`"schema":{"type":"object"...`) (#101).
- `testInfoOmitsSchemaWhenEndpointHasNone` - Asserts an endpoint without a declared schema emits an info_response that contains no `"schema"` key.
- `testDiscoveryReplies` - Asserts PING, INFO, and STATS discovery requests each produce a PUB to the reply inbox with the matching `io.nats.micro.v1.{ping,info,stats}_response` type.
- `testEndpointHandlesRequests` - Asserts an endpoint request is dispatched to the handler and its array result is JSON-encoded and published to the reply subject (`PUB _INBOX.req` / `{"echo":"hello"}`).
- `testEndpointResponseEncodeFailureDoesNotTearDownDispatch` - Asserts a handler whose result cannot be json_encoded yields a controlled 500 (Nats-Service-Error) for that request without aborting the dispatch loop, so a subsequent good endpoint is still served (#97).
- `testStopUnsubscribesAll` - Asserts stop() writes UNSUB for both the discovery SID (1) and the endpoint SID (13).
- `testGroupedEndpointHierarchy` - Asserts nested addGroup('svc')->addGroup('v1') prefixes the endpoint subject to `svc.v1.echo` (SUB with queue group `q`), routes the request to the handler, and reflects the subject in the stats snapshot.
- `testGroupJoinHandlesEmptySegments` - Asserts empty group prefixes / empty subjects are trimmed so subjects collapse to `echo` and `svc` rather than containing empty dot segments.
- `testSchemaDiscoveryResponse` - Asserts the service subscribes to $SRV.SCHEMA and answers a SCHEMA discovery request with an `io.nats.micro.v1.schema_response` containing the endpoint schema.
- `testStatsIncludeDetailedMetrics` - Asserts endpoint stats track num_requests (2), num_errors (1), last_error message, non-negative processing/average processing time, and that the `started` timestamp is stable across snapshots.
- `testStartedTimestampIsUtcRegardlessOfDefaultTimezone` - With the process default timezone set to Pacific/Kiritimati (+14), the stats `started` timestamp still parses as UTC within a minute of now, so its hardcoded Z suffix is truthful (ADR-32, #132).
- `testHandlerCanRespondWithCustomServiceError` - Asserts a thrown ServiceError(429,...) surfaces its code/description/body in the micro-spec error headers and body (not a generic 500) and is recorded as one error with the custom description in stats.
- `testHandlerErrorResponseDoesNotLeakExceptionMessage` - Asserts a generic handler exception returns a sanitized "Internal server error" 500 to the requester (no raw exception text), while the real message is retained server-side in `last_error`/STATS.
- `testStatsOmitsNonSpecAliasKeys` - Asserts endpoint stats use spec field names (`num_requests`, `num_errors`) and no longer expose the non-spec aliases (`requests`, `errors`).
- `testServiceRejectsInvalidName` - Asserts creating a service with a dotted name throws InvalidArgumentException mentioning "Service name".
- `testServiceRejectsNonSemverVersion` - Asserts creating a service with a non-semver version ('v1') throws InvalidArgumentException mentioning "semantic version".
- `testValidationRejectionEmitsRequestEnd` - Asserts a request rejected by the request validator still emits the full observer sequence `request_start, request_error, request_end` so opened spans are not leaked.
- `testStartRollsBackAndClearsStateOnPartialFailure` - Asserts that when a later endpoint's subscribe fails during start(), the partial state is rolled back (empty subscriptionSids) and the service is left not-started.
- `testResetClearsStats` - Asserts reset() zeroes num_requests, num_errors, processing_time, average_processing_time and nulls last_error in the stats snapshot.
- `testRequestValidatorCanRejectRequests` - Asserts a request validator returning an error string blocks the handler, publishes a VALIDATION_ERROR `io.nats.micro.v1.error` envelope with the message, and counts the request as one request and one error.
- `testObserversReceiveLifecycleEvents` - Asserts observers receive request_start then request_end events carrying the correlation id (from X-Request-Id header) and the request subject.
- `testSuccessfulRequestWithHeadersAndNoObserversRepliesCorrectly` - Asserts a successful request carrying headers on a service with zero observers still publishes the correct reply and counts one request / zero errors; pins the #140 path where the observer context (header parse) is skipped entirely (no counting seam exists, so behavior is the observable).
- `testWithSchemaValidatorUsesAdapter` - Asserts withSchemaValidator(BasicJsonSchemaValidator) validates the payload against the endpoint schema and emits a VALIDATION_ERROR with a type-mismatch message (`$.id must be integer, got string`).
- `testValidationReplyToleratesNonUtf8CorrelationHeader` - Asserts a requester-supplied non-UTF-8 X-Request-Id on a schema-failing request still gets its VALIDATION_ERROR reply published, with the poisoned correlation id omitted rather than encoded, and no JsonException escaping into the shared dispatch loop (#97).
- `testServiceErrorCrLfCodeIsSanitizedAndReplyStillSent` - Asserts a ServiceError whose code contains CR/LF is collapsed onto one header line (`Nats-Service-Error-Code:400 X-Injected: 1`), so header injection is blocked, the error reply is still sent, and the header build never throws out of the dispatch loop.
- `testReplyPublishConnectionFailureIsRecordedNotEscaping` - Asserts a reply publish that fails because the connection closed while a successful handler ran is recorded on the endpoint (one request, one error, `last_error` containing "not open") and emits a trailing request_error instead of escaping into the shared dispatch loop.
- `testHandlerErrorWithFailedErrorReplyCountsSingleError` - Asserts a handler error whose error-reply publish also fails is counted as exactly one error (so num_errors never exceeds num_requests) and emits a single request_error carrying the handler's own code before the terminal request_end, with only `last_error` recording the later publish failure.
- `testErrorEnvelopeIncludesCorrelationIdFromHeaders` - Asserts a handler error's JSON envelope carries `"code":"HANDLER_ERROR"` and the `correlation_id` taken from the request's X-Request-Id header.
- `testEndpointAcceptsObjectHandlerAdapter` - Asserts an endpoint handler object implementing ServiceEndpointHandlerInterface is invoked (reply contains `obj:hello`).
- `testEndpointAcceptsClassStringHandlerAdapter` - Asserts a class-string handler is auto-instantiated and invoked (reply contains `class:hello`).
- `testEndpointRejectsInvalidObjectHandlerAdapter` - Asserts passing an object that does not implement the handler interface throws InvalidArgumentException ("Unsupported service endpoint handler").
- `testRunProcessesAndStopsOnTimeout` - Asserts run(0.03) processes an incoming request (publishes `run:hello`) and auto-stops on timeout, unsubscribing the service SIDs (`UNSUB 1`).
- `testRunSupportsExternalCancellation` - Asserts run() driven by a DeferredCancellation stops when externally cancelled and still unsubscribes both discovery (1) and endpoint (13) SIDs.
- `testEndpointDefaultsToSpecQueueGroup` - Asserts endpoints default to the micro-spec queue group `q` (`SUB svc.echo q 13`) while discovery subscriptions stay non-queued (`SUB $SRV.PING 1`).
- `testEndpointEmptyStringQueueGroupOptsOut` - Asserts an empty-string queue group produces a plain non-queued SUB (`SUB svc.echo 13`, no `SUB svc.echo q`).
- `testEndpointNullQueueGroupOptsOut` - Asserts a null queue group likewise produces a plain non-queued SUB and omits the default `q` queue group.
- `testRunPassesCancellationIntoSocketRead` - Asserts the run loop passes a real cancellation into the idle socket read so the read is torn down rather than orphaned (lastReadHadCancellation true; startedReads > 0 and equal to resolvedReads).
- `testRunLeavesConnectionReusableAfterTimeout` - Asserts that after run() times out the shared NatsConnection's `readInProgress` flag is false, leaving the connection reusable (read not orphaned).
- `testAddEndpointRejectsDuplicateSubject` - Asserts adding a second endpoint on the same subject throws InvalidArgumentException.
- `testAddEndpointRejectsEmptyName` - Asserts adding an endpoint with a blank/whitespace name throws InvalidArgumentException ("name must be non-empty and match").
- `testAddEndpointRejectsEmptySubject` - Asserts adding an endpoint with an empty subject throws InvalidArgumentException ("subject must not be empty").
- `testClassHandlerWithRequiredConstructorArgIsRejected` - Asserts a class-string handler requiring a constructor argument is rejected with InvalidArgumentException ("could not be instantiated") rather than a raw ArgumentCountError.
- `testStopToleratesClosedConnection` - Asserts stop() after the connection is disconnected completes without aborting (unsubscribe() releases local state silently on a closed connection per #116) and still clears subscriptionSids.
- `testRunStopsWhenConnectionIsUnrecoverable` - Asserts run() with no timeout returns (within a 2s bound) once the connection becomes unrecoverable (EOF, reconnect disabled) instead of busy-spinning.
- `testDiscoveryHandlerSwallowsEncodeFailure` - Asserts an INFO discovery payload that fails to JSON-encode (invalid-UTF-8 description) is swallowed inside the discovery handler so processIncoming still completes (1 frame).
- `testStartIsIdempotentWhenAlreadyStarted` - Asserts calling start() on an already-started service is a no-op, producing no additional writes/SUB frames.
- `testServiceErrorWithNullBodyIncludesCorrelationIdFromHeader` - Asserts a ServiceError thrown with a null body still emits an error payload including the correlation_id from the header plus `"code":"422"` and the `Nats-Service-Error-Code:422` header.
- `testHandlerReturningNullSendsNoReply` - Asserts a handler returning null runs but publishes no reply (no `PUB _INBOX.req`), while still incrementing num_requests to 1.
- `testDrainFiresDoneHandler` - Asserts drain() fires the onDone handler exactly once.
- `testDoneHandlerExceptionIsSwallowed` - Asserts an exception thrown by the onDone handler during stop() is swallowed (stop completes and the started flag is cleared).
- `testDiscoveryMessageWithoutReplyToIsIgnored` - Asserts a PING discovery message with no reply subject is processed (1 frame) but produces no PUB $SRV response.
- `testEndpointRequestWithNoReplyToSendsNoResponse` - Asserts a fire-and-forget endpoint request (no reply subject) still runs the handler but emits no PUB.
- `testObserverExceptionIsSwallowed` - Asserts an observer that throws does not interrupt request handling; processIncoming completes and the handler still replies (`PUB _INBOX.req` / `ok`).
- `testRunRejectsNonPositiveTimeout` - Asserts run(0.0) throws InvalidArgumentException ("timeout must be greater than zero").
- `testRunWithOnlyTimeoutUsesTimeoutCancellation` - Asserts run() with only a timeout and no external cancellation returns via the TimeoutCancellation path and stops the service (UNSUB emitted).
- `testStatsHandlerExceptionIsSwallowed` - Asserts a stats supplier that throws is swallowed: the endpoint entry is present (has num_requests) but omits the `data` key.
- `testRunBreaksImmediatelyWhenCancellationAlreadyRequested` - Asserts run() exits promptly without hanging when given an already-cancelled cancellation, still stopping the service (UNSUB emitted).
- `testStartRollbackSwallowsUnsubscribeFailureOnClosedConnection` - Asserts start() failing on a bad endpoint subject rolls back already-subscribed SIDs while swallowing the rollback unsubscribe failures (forced via failing UNSUB writes, since #116 made unsubscribe() a silent no-op on a not-open connection), leaving subscriptionSids empty and started false.
- `testDrainSwallowsUnsubscribeWriteFailure` - Asserts drain() swallows per-SID unsubscribe exceptions (forced via failing UNSUB writes on an otherwise-open connection, since #116 removed the closed-connection throw) and still clears subscriptionSids and the started flag.
- `testDrainToleratesFlushFailureOnClosedConnection` - Asserts drain() swallows a flush() failure caused by a closed connection and still clears state (started false, subscriptionSids empty).
- `testRunWithBothTimeoutAndExternalCancellationUsesCompositeCancellation` - Asserts run() given both a timeout and an external cancellation uses the CompositeCancellation branch, stopping the service when the external cancel fires first (UNSUB emitted).
- `testValidationErrorReplyPublishFailureIsContained` - Asserts a failing VALIDATION_ERROR reply publish stays contained inside the dispatch callback: the frame still completes, the handler never runs, the endpoint records one error with `last_error` "bad input", and the terminal request_end still fires (#97).
- `testStopSwallowsUnsubscribeWriteFailureAndStillFiresDone` - Asserts stop() swallows per-SID unsubscribe write failures, still clears subscriptionSids and the started flag, fires the onDone handler once, and leaves the service restartable.
- `testRunBacksOffAndKeepsReadingAfterDispatchError` - Asserts a dispatch-level handler failure on the shared open connection does not kill run(): the loop backs off and reads again, then exits cleanly on cancellation with the connection still Open and the service stopped (UNSUB emitted).
- `testRunCancellationDuringDispatchErrorBackoffStopsService` - Asserts cancelling run() while it sits in the dispatch-error backoff exits promptly instead of draining every queued poison frame, leaving the connection Open and the service stopped (UNSUB emitted).
- `testEndpointRejectsClassStringNotImplementingHandlerInterface` - Asserts a class-string endpoint handler that does not implement ServiceEndpointHandlerInterface is rejected at registration with a message naming both the class and the required interface, rather than being wrapped into a handler that fails at request time.

### tests/Unit/StreamSourceTest.php
- `testMirrorMinimal` - Asserts StreamSource::mirror('ORIGIN')->toArray() yields exactly ['name' => 'ORIGIN'].
- `testMirrorWithStartSeq` - Asserts startSeq(42) on a mirror sets opt_start_seq=42 alongside the name.
- `testMirrorWithStartTime` - Asserts startTime() on a mirror sets opt_start_time to the given RFC3339 string.
- `testSourceWithFilterSubject` - Asserts StreamSource::source('ORDERS')->filterSubject('orders.>') emits name and filter_subject keys.
- `testSourceWithExternal` - Asserts external(api, deliver) emits an external sub-array with both 'api' and 'deliver' keys.
- `testExternalWithoutDeliver` - Asserts external(api) with no deliver emits an external sub-array containing only the 'api' key.
- `testFullyConfiguredSource` - Asserts a source with startSeq, filterSubject, and external(api, deliver) serializes to the full expected array with all keys including the nested external map.

### tests/Unit/SubscriptionQueueTest.php
- `testSubscribeQueueReturnsSidAndFetchesMessage` - `subscribeQueue('events')` returns a `SubscriptionQueue` with sid 1 and `fetch()` returns the message with payload 'hello' and subject 'events'.
- `testMessageDeliveredDuringSubscribeAwaitReachesTheQueue` - A message dispatched while the SUB write is still suspended (queue object not constructed yet) is buffered and reaches the returned queue instead of being silently dropped (#129).
- `testFetchReturnsNullWhenNoMessages` - `fetch()` returns null when no message is available.
- `testNextReturnsBufferedMessageImmediately` - after `processIncoming()` pre-buffers a message, `next()` returns it immediately (payload 'abc').
- `testNextReturnsNullOnTimeout` - with a 0.01s timeout and no messages, `next()` returns null.
- `testNextWithoutTimeoutRunsSingleCycleAndReturnsMessage` - with default timeout 0, `next()` runs a single processIncoming cycle and surfaces the message (payload 'xyz').
- `testNextWithoutTimeoutReturnsNullWhenEmpty` - with default timeout 0 and no messages, `next()` returns null after one empty cycle rather than blocking.
- `testFetchAllCollectsMultipleMessages` - `fetchAll()` with a 0.1s timeout collects all three messages in order ('a','b','c').
- `testFetchAllRespectsLimit` - `fetchAll(2)` collects only the first two messages ('a','b').
- `testSubscribeQueueWithQueueGroup` - `subscribeQueue('work','workers')` emits `SUB work workers 1` and `fetch()` returns 'job1'.
- `testSetTimeoutReturnsSelf` - `setTimeout(5.0)` returns the same queue instance.
- `testFetchReturnsAlreadyBufferedMessage` - after `processIncoming()` buffers a message, `fetch()` returns it directly (payload 'hi') without another read.
- `testNextWithTimeoutReturnsMessageArrivingDuringWait` - with a 0.2s timeout and no pre-buffer, `next()` breaks the bounded wait as soon as the message arrives (payload 'abc') rather than running to timeout.
- `testFetchAllWithoutTimeoutCollectsBufferedMessages` - `fetchAll()` with no timeout (null cancellation path) collects both buffered messages ('a','b').
- `testFetchAllReturnsEarlyWhenBufferedMeetsLimit` - with one pre-buffered message, `fetchAll(1)` returns early from the buffered drain alone (one message 'a').
- `testFetchDoesNotBlockOnIdleSubject` - against a transport that blocks when empty, `fetch()` returns null within a 2s bound and `lastReadHadCancellation` is true, proving the read is cancellation-bounded.
- `testNextWithDefaultTimeoutDoesNotBlockOnIdleSubject` - with default timeout 0 on a blocking transport, `next()` returns null within a 2s bound without parking the fiber.
- `testNextWithNegativeTimeoutDoesNotBlockOnIdleSubject` - with timeout -1.0 on a blocking transport, `next()` returns null within a 2s bound without blocking.
- `testFetchAllWithDefaultTimeoutDoesNotBlockOnIdleSubject` - with no setTimeout on a blocking transport, `fetchAll()` bounds its read and returns `[]` within a 2s bound rather than parking.
- `testFetchAllDoesNotBailOnTransientEmptyReadWithinTimeout` - a transient 0-frame read between two deliveries does not end `fetchAll(2)` early while timeout remains; both 'msg1' and 'msg2' are collected.
- `testEnqueueBoundsBacklogWithDropOldest` - with cap 2 and DropOldest policy, a third enqueue drops the oldest ('a'); fetchAll yields `['b','c']`.
- `testEnqueueDropsNewestWhenPolicyIsDropNewest` - with cap 2 and DropNewest policy, a third enqueue drops the newest ('c'); fetchAll yields `['a','b']`.
- `testEnqueueDropOldestIncrementsDroppedCountAndNotifiesErrorListener` - with cap 2 and DropOldest, each overflow increments `droppedCount()` monotonically (0 -> 1 -> 2) and invokes the client's errorListener with "Slow consumer on sid 99: dropped oldest message" (#134).
- `testEnqueueDropNewestIncrementsDroppedCountAndNotifiesErrorListener` - with cap 2 and DropNewest, the discarded (never-enqueued) third message increments `droppedCount()` to 1 and invokes the client's errorListener with "Slow consumer on sid 99: dropped newest message" (#134).
- `testUnsubscribeSendsUnsubForOwnSid` - `unsubscribe()` writes `UNSUB {sid}` for the queue's own sid.
- `testEnqueueThrowsOnOverflowWhenPolicyIsError` - with cap 2 and Error policy, the third enqueue throws `NatsException` with message "Subscription queue overflow for sid 99", and the drop is observable: droppedCount() increments to 1 and the error listener is notified before the throw (#134/#159).
- `testCloseSendsUnsubForOwnSid` - `close()` (alias of unsubscribe) writes `UNSUB {sid}` for the queue's own sid.
- `testNextWithTimeoutReturnsNullWhenNoMessageArrivesBeforeDeadline` - with a 0.02s timeout on a blocking transport, the TimeoutCancellation fires inside processIncoming (CancelledException caught) and `next()` returns null.
- `testFetchAllFinalDrainCollectsConcurrentlyEnqueuedMessage` - while `fetchAll()` is suspended in processIncoming, a concurrent fiber enqueues a 'late' message; after the timeout cancels, the final drain loop collects it (one message 'late').

### tests/Unit/WebSocketFrameCodecTest.php
- `testEncodeMaskedFrameRoundTrips` - Encodes a masked client binary frame and asserts the header bytes (0x82, mask bit + length 6), then decode() returns one frame with the right opcode, payload "PING\r\n", fin=true, and an emptied buffer.
- `testAcceptKeyMatchesRfcExample` - Asserts acceptKey() produces the exact Sec-WebSocket-Accept value from the RFC 6455 §1.3 sample nonce.
- `testDecodeKeepsIncompleteTrailingFrame` - Feeding all-but-last byte yields no frames and an untouched buffer; appending the final byte then decodes the full "hello" frame and clears the buffer.
- `testDecodeReturnsMultipleFrames` - Concatenates three frames (binary, ping, binary) and asserts decode() returns all three with correct payloads and the ping opcode.
- `testDecodeExtended16BitLengthFrame` - Encodes a 300-byte payload, asserts the 16-bit length marker (126) is used, and decode() returns the full payload.
- `testDecodeReportsFragmentationFlag` - Hand-crafts a non-final (FIN=0) binary frame and asserts decode() reports fin=false with payload "abc".
- `testDeflateInflateRoundTrip` - Asserts deflate() shrinks a repetitive payload (output differs and is shorter) and inflate() restores the original exactly.
- `testInflateEnforcesDecompressedSizeCap` - Asserts inflate() with a small explicit cap throws ProtocolException ("exceeded the maximum") on a decompression bomb (~1 MiB of one byte, compressed to under the cap) instead of allocating the full output unboundedly (#121).
- `testInflateReturnsFullPayloadWithinCap` - Asserts inflate() returns a 64 KiB INCOMPRESSIBLE payload (random_bytes, deflating to slightly MORE than 64 KiB so its compressed input spans ~9 INFLATE_INPUT_CHUNK_BYTES slices) byte-identically under a matching cap - genuinely iterating the bounded multi-slice loop and reassembling across it, so the cap never corrupts or truncates legitimate output. The test also asserts the compressed input exceeds several 8192-byte slices, proving the loop is not collapsing to a single pass (#121).
- `testInflateAbortsMidLoopWhenMultiSliceOutputExceedsCap` - Asserts inflate() enforces the decompressed-size cap WHILE the bounded loop iterates: a 96 KiB incompressible payload (compressed input spanning ~13 slices) inflated under a 16 KiB cap throws ProtocolException ("exceeded the maximum") partway through the loop rather than reassembling the whole output first (#121).
- `testCompressedFrameRoundTrip` - Encodes a deflated payload as a compressed frame, asserts the RSV1 bit is set, and decode() reports rsv1=true with payload that inflates back to the original.
- `testEncodeRejectsBadMaskKey` - Asserts encode() throws ProtocolException when given a mask key that is not 4 bytes.
- `testEncode64BitLengthFrameRoundTrips` - For a 65536-byte payload asserts the 127 length marker and an 8-byte big-endian length equal to 65536, then decode() reconstructs the payload and empties the buffer.
- `testDecode64BitLengthHeaderIncompleteWaits` - A 127-marker header with only 3 of 8 length bytes yields no frames and an unchanged buffer (decode waits for the full 64-bit length).
- `testDecode16BitLengthHeaderIncompleteWaits` - A 126-marker header with only 1 of 2 length bytes yields no frames and an unchanged buffer (decode waits for the full 16-bit length).
- `testDecodePayloadLengthOutOfBoundsReportsTerminalAfterValidFrames` - Asserts a declared length of MAX_FRAME_PAYLOAD+1 is no longer thrown mid-batch (which discarded frames already decoded from the same read) but reported through the `$terminal` out-param: decode() still returns the valid frame parsed before the violation, consumes only that frame's bytes, and leaves the poison header in the buffer.
- `testDecodeRejectsMaskedServerFrameUnlessAllowed` - Asserts RFC 6455 5.1 is enforced by default: a masked server-to-client frame yields no frames and a terminal ProtocolException mentioning "masked", while the explicit allowMasked mode (client-written frames, server-side harnesses) still unmasks it and reports no terminal error.
- `testDecodeRejectsFragmentedOrOversizedControlFrames` - Asserts RFC 6455 5.5 is enforced: a fragmented (FIN=0) PING and a PING carrying 126 payload bytes each set a terminal ProtocolException mentioning "control frame" instead of being answered and spliced into an in-progress fragmented message.
- `testDecodeMaskedFrameWaitsForMaskKey` - A masked frame header with only 2 of 4 mask-key bytes yields no frames and an unchanged buffer.
- `testInflateInvalidDataThrows` - With PHPUnit's error handler disabled, asserts inflate() on garbage input throws ProtocolException matching "inflate compressed WebSocket frame".
- `testFrameRequiredBytesComputesFullWireSizePerLengthForm` - Asserts frameRequiredBytes() returns the exact full wire size for 7-bit unmasked, 7-bit masked, 16-bit and 64-bit length frames - the 16-bit masked one from its 4 header bytes with the mask key still in flight, the 64-bit one from a 10-byte prefix alone (#164).
- `testFrameRequiredBytesReturnsNullOnIncompleteHeader` - Asserts frameRequiredBytes() returns null for an empty buffer, a single header byte, and 126/127-marker headers missing extended-length bytes (#164).
- `testFrameRequiredBytesRejectsOversizedDeclaredLength` - Asserts frameRequiredBytes() throws ProtocolException with the exact message "WebSocket frame payload length out of bounds: {length}" for a declared length of MAX_FRAME_PAYLOAD+1, pinning the offending length as the operator's diagnostic for a hostile frame (#164).
- `testParseFrameHeaderComputesFullWireSizePerLengthForm` - Asserts parseFrameHeader() reports headerBytes+payloadLength equal to the full wire size for 7-bit unmasked/masked, 16-bit and 64-bit length frames, surfacing fin/rsv1/opcode/masked and the mask key (#164).
- `testParseFrameHeaderReturnsNullOnIncompleteHeader` - Asserts parseFrameHeader() returns null for an empty buffer, a single header byte, 126/127-marker headers missing extended-length bytes, and a masked header missing mask-key bytes (#164).
- `testParseFrameHeaderRejectsOversizedDeclaredLength` - Asserts parseFrameHeader() throws ProtocolException with the exact message "WebSocket frame payload length out of bounds: {length}" the moment the hostile length bytes arrive, even with the mask key still in flight (#164).
- `testDeprecatedUnmaskStillInvertsEncodeMasking` - Asserts the deprecated unmask() helper still inverts the masking encode() applies, restoring the original payload from an encoded client frame's masked bytes: it is production-dead (masked server frames are now a terminal RFC 6455 5.1 violation) but public since 2.7.x, so it stays behavior-pinned until a major release removes it (#164).
- `testInflateInvalidDataSuppressesNativeWarningForRespectfulHandlers` - Installs a handler that respects error_reporting() (the @ operator) and asserts inflate() on garbage still surfaces a typed ProtocolException rather than leaking an ErrorException from inflate_add().
- `testParseFrameHeaderDecodesTwoByteNonFinalTextHeader` - Asserts parseFrameHeader() decodes a complete two-byte 7-bit header (the minimum sufficient prefix) of a non-final text frame (byte1=0x01), pinning the < 2 availability boundary and the fin(0x80)/rsv1(0x40)/opcode(0x0F) bit-field extraction - byte1=0x01 makes those masks disagree so a mutated mask or comparison flips a reported field.
- `testFrameRequiredBytesSizesTwoByteHeader` - Asserts frameRequiredBytes() returns 5 for a complete two-byte 7-bit header (FIN+binary, length 3), pinning the < 2 availability boundary against a <= 2 off-by-one that would report a fully-headed frame as unsized.
- `testFrameRequiredBytesAcceptsZeroLengthFrame` - Asserts frameRequiredBytes() returns 2 for a zero-length frame (empty ping), pinning the lower length bound (< 0) against a <= 0 mutation that would reject a valid empty frame.
- `testFrameRequiredBytesAcceptsExactlyMaxDeclaredLength` - Asserts frameRequiredBytes() accepts a frame declaring exactly MAX_FRAME_PAYLOAD (returning 10 + MAX from its 64-bit-length header), pinning the upper bound (> MAX) against a >= MAX mutation that would reject the boundary length.
- `testParseFrameHeaderDecodes16BitHeaderAtExactlyFourBytes` - Asserts parseFrameHeader() decodes a 16-bit (126-marker) header the moment its 4 length bytes arrive (payloadLength 300, headerBytes 4), pinning the < 4 boundary against a <= 4 off-by-one.
- `testParseFrameHeaderAcceptsZeroLengthFrame` - Asserts parseFrameHeader() accepts a zero-length frame (payloadLength 0), pinning the lower length bound against a <= 0 mutation.
- `testParseFrameHeaderAcceptsExactlyMaxDeclaredLength` - Asserts parseFrameHeader() accepts a frame declaring exactly MAX_FRAME_PAYLOAD from its 64-bit-length header alone, pinning the upper bound against a >= MAX mutation.
- `testParseFrameHeaderReadsMaskKeyAtExactHeaderFit` - Asserts parseFrameHeader() surfaces a masked frame's 4-byte mask key as soon as header+key (6 bytes == offset+4) are buffered, pinning the available < offset + 4 boundary against +5 / <= off-by-one mutations that would report null when the key is already present.
- `testGenerateClientKeyEncodesSixteenRandomBytes` - Asserts generateClientKey() base64-encodes exactly 16 random bytes (the decoded length, since 15 and 17 bytes both base64 to 24 chars), pinning the RFC 6455 §4.1 key size against +/-1 mutations.

### tests/Unit/WebSocketTransportTest.php
- `testIsTlsAwareAndInactiveBeforeConnect` - Asserts the transport implements TlsAwareTransportInterface and reports tlsActive()=false before connecting.
- `testReadLineReturnsEmptyWithoutSocket` - Asserts readLine() resolves to '' (not EOF) when no socket is connected yet.
- `testWriteWithoutSocketThrowsTransportClosed` - Asserts write() without a connected socket throws TransportClosedException instead of silently succeeding (#124).
- `testUpgradeTlsIsNoOp` - Asserts upgradeTls() resolves without error and leaves TLS inactive (wss negotiates TLS during connect, not via upgrade).
- `testBuildUpgradeRequestWithCustomHeadersAndCompression` - Asserts the built upgrade request contains the GET line, Host with port, Sec-WebSocket-Key, the permessage-deflate extension offer, both custom headers, and ends with the blank-line terminator.
- `testBuildUpgradeRequestRejectsReservedAndStripsCrLf` - Asserts a custom Host override is ignored (real Host kept, "evil" absent), CR/LF is stripped from custom values so no header is smuggled, and no compression offer appears when disabled.
- `testConnectRejectsDsnWithoutHost` - Asserts connect() throws ConnectionException ("Invalid WebSocket DSN") for a DSN lacking a host before any socket attempt.
- `testConnectAppendsQueryStringToPathBeforeSocketAttempt` - Asserts a ws:// DSN with a query string builds the path then fails at the socket connect with Amp ConnectException (proving path-building ran).
- `testConnectBuildsTlsContextForWssSchemeBeforeSocketAttempt` - Asserts a wss:// DSN enters the secure branch and builds a TLS context, then fails at the socket connect with Amp ConnectException.
- `testConnectCallsSetupTlsOnWssAndThrowsWhenServerIsPlainTcp` - With a local plain-TCP listener, asserts wss:// connect succeeds at TCP but setupTls() throws TlsException and tlsActive() stays false.
- `testBuildUpgradeRequestSanitizesHeaderNamesAgainstInjection` - Asserts CR/LF and ':' are stripped from custom header NAMES (and values) so no forged header lines appear, sanitized headers survive on single lines, and exactly one header/body terminator exists.
- `testDrainReassemblesFragmentedMessageWithinBound` - Using reflection on readBuffer/drainDataFrames, asserts a binary+two-continuation fragmented message within the cap reassembles to "PING\r\n".
- `testDrainRejectsOversizedFragmentedMessage` - Asserts drainDataFrames() throws ProtocolException ("exceeded the maximum") when continuation frames push reassembly past maxMessageBytes.
- `testReadLineDecodesBinaryFrameFromSocket` - Over a real loopback socket, asserts readLine() decodes a server-written unmasked binary frame to "PING\r\n".
- `testReadLineAnswersPingWithPongAndContinues` - Asserts a server PING is answered inline (readLine returns the subsequent DATA frame) and the server receives a masked PONG carrying the original ping payload "hb".
- `testReadLineInflatesCompressedFrameFromSocket` - Asserts readLine() inflates an RSV1 permessage-deflate compressed frame read from the socket back to the original INFO payload.
- `testReadLineReassemblesLargeFrameDeliveredInSmallChunks` - Over a real loopback socket, asserts a 256 KiB frame of distinct counter bytes written in 8 KiB pieces is returned by readLine() byte-identical (pins the #164 chunk-accumulation join).
- `testReadLineKeepsPartialNextFrameAcrossReads` - Asserts readLine() returns a complete frame while half of the next frame is already buffered, then returns that next frame once its remaining bytes arrive (pins #164 head-frame completion tracking).
- `testReadLineAnswersPingBetweenFragmentsAndReassembles` - Asserts a PING interleaved between the fragments of a fragmented message is answered with a masked PONG carrying "hb" while the fragments still reassemble to "HELLO" (#164).
- `testDrainSizesLarge64BitFrameForSpillButLeaves16BitFrameOnBuffer` - White-box pin (both paths deliver identical bytes, so internal state must be inspected): asserts drainDataFrames() sizes an incomplete 64-bit-length frame for chunk-list spill (readFrameRequired = full wire size) but never sizes an incomplete 16-bit-length frame, which stays on the batch-decode `.=` path (#164).
- `testReadLineAssembles64BitLengthFrameDeliveredInOneByteReads` - Read-boundary torture pin: with a ScriptedChunkSocket (`tests/Support/ScriptedChunkSocket.php`, one byte per read, so TCP coalescing cannot hide the boundaries), asserts a 64-bit-length 65536-byte frame of distinct counter bytes delivered one byte at a time is returned by readLine() byte-identical - exercising the spanning-consume join across the chunk list (#164).
- `testReadLineReassemblesFragmentsWithHeadersSplitAcrossReads` - Read-boundary torture pin: with a ScriptedChunkSocket, asserts a fragmented message whose middle unmasked 16-bit-length continuation header is cut inside its extended length bytes, whose payload spans further reads through the chunk-list consume path, and whose final continuation header is cut between its two bytes, still reassembles to "HEAD-"+mid+"TAIL" byte-identical (#164).
- `testReadLineAssemblesLargeFrameDeliveredInOneByteReads` - Read-boundary torture pin: with a ScriptedChunkSocket, asserts an unmasked 64-bit-length 70000-byte frame delivered one byte at a time - sized from its length bytes long before the rest arrives, so the frame spills to chunk-list accumulation and is assembled by the spanning-consume path - is returned by readLine() byte-identical (#164).
- `testReadLineRejectsMaskedServerFrameOnSpillPath` - Asserts a MASKED server frame large enough to take the 64-bit-length spill path is a terminal RFC 6455 5.1 violation (ProtocolException naming "masked") instead of being silently unmasked and accepted.
- `testReadLineThrowsOnCloseFrameFromSocket` - Asserts a server CLOSE frame causes readLine() to throw TransportClosedException.
- `testReadLineEchoesCloseFrameOnServerInitiatedClose` - Asserts a server CLOSE frame (status 1001) makes the client write back a masked CLOSE echo mirroring the status code (RFC 6455 5.5.1) before readLine() still throws TransportClosedException, so the connection layer keeps reconnecting (#161).
- `testReadLineReturnsSameChunkDataThenDefersClose` - With a ScriptedChunkSocket delivering [data frame][CLOSE] in one read, asserts readLine() returns the data bytes (not discarded by the trailing close), the masked CLOSE echo mirroring status 1001 is still written (#161), and the deferred close surfaces on the FOLLOWING readLine() as TransportClosedException (#115).
- `testCloseCompletesWhenCloseFrameWriteWedgesOnBackpressure` - With a WedgedWriteSocket (`tests/Support/WedgedWriteSocket.php`) whose write() suspends on backpressure exactly as Amp's does, asserts close() still reaches the socket close within a bounded await, having attempted the courtesy Close frame once, so recovery cannot deadlock behind a stalled peer.
- `testReadLinePreservesSameReadDataWhenPongWriteFails` - With a ScriptedChunkSocket delivering [data frame][PING] in one read and failing all writes, asserts readLine() still returns the already-decoded data bytes (the pong answer goes out on its own fiber, so its failure cannot discard them) and the peer's death surfaces as TransportClosedException on the NEXT readLine().
- `testReadLineNotStalledByWedgedPongWrite` - With a WedgedWriteSocket delivering [data frame][PING] in one read, asserts the read path is not parked behind the outbound pong (the data frame is returned promptly, the pong write is still attempted once) and close() afterwards releases the wedged writer.
- `testCloseWithoutSocketAndRepeatedCloseAreNoOps` - Asserts close() on a never-connected transport is a clean no-op, the first close over a real socket closes it and writes the Close frame exactly once, and a repeated close neither re-attempts the frame write nor errors.
- `testCloseStillWritesCloseFrameOnResponsiveSocket` - Asserts that on a responsive socket close() still sends the RFC 6455 Close frame (masked, opcode 0x8, empty payload, FIN set) before closing, so the wedge fix did not drop the courtesy frame from the healthy path.
- `testReadLineReturnsSpilledFrameThenDefersCloseSharedWithFinalRead` - Asserts the deferred-close path integrates with the #164 spill: a 64-bit-length 70000-byte frame whose final read also carries a CLOSE frame returns the full spilled payload first, then surfaces the deferred close on the next readLine() - the spilled bytes are not lost (#115).
- `testReadLineReturnsSameChunkDataThenDefersFragmentationViolation` - With a ScriptedChunkSocket delivering [complete data frame][fragment start][new data frame mid-fragmentation] in one read, asserts readLine() returns the complete message first (not discarded by the RFC 6455 5.4 violation) and the ProtocolException ("fragment") surfaces on the FOLLOWING readLine() - deferred exactly like the close, so the same-batch data is never lost (#115).
- `testReadLineReturnsSameChunkDataThenDefersOrphanContinuation` - With a ScriptedChunkSocket delivering [complete data frame][orphan continuation frame] in one read, asserts readLine() returns the complete message first and the ProtocolException ("continuation") surfaces on the FOLLOWING readLine() - the orphan-continuation violation defers like the close rather than discarding the already-decoded data (#115).
- `testDrainFailsWhenDataFrameArrivesMidFragmentation` - Using reflection on readBuffer/drainDataFrames, asserts a new data frame arriving while a fragmented message is in progress - with no data decoded earlier in the same batch to return first - throws ProtocolException (RFC 6455 5.4) right away instead of silently overwriting the partial message (#115).
- `testDrainFailsOnOrphanContinuationFrame` - Using reflection on readBuffer/drainDataFrames, asserts a continuation frame with no fragmented message in progress - with no preceding data to return - throws ProtocolException (RFC 6455 5.4) right away instead of being silently dropped (#115).
- `testReadLineReturnsSameReadDataBeforeOversizedLengthViolation` - Asserts the #115 deferral also covers codec strictness violations: a read carrying [valid MSG][frame declaring an out-of-bounds 64-bit length] returns the MSG bytes first and surfaces the ProtocolException ("out of bounds") on the NEXT readLine().
- `testReadLineRejectsFragmentedControlFrameInsteadOfCorruptingReassembly` - Asserts a fragmented PING arriving mid-fragmented-message is a terminal RFC 6455 5.5 violation (ProtocolException naming "control frame") with no corrupted payload delivered first, instead of answering the pong and splicing the ping's continuation bytes into the data message.
- `testReadLineRejectsRsv1WithoutNegotiatedCompression` - Asserts an RSV1 data frame received when permessage-deflate was never negotiated fails the connection with a named ProtocolException ("permessage-deflate was not negotiated") rather than being blindly inflated (RFC 6455 5.2).
- `testWriteSurfacesSocketFailureThroughReturnedFuture` - Asserts a throwing socket write surfaces through the future returned by write() and never as a synchronous throw, so callers that queue the future and await later still observe the failure (#136).
- `testAmpSocketTransportWriteSurfacesSocketFailureThroughReturnedFuture` - Asserts the TCP AmpSocketTransport honors the same write error-path contract: a throwing socket write surfaces through the returned future, never synchronously (#136).
- `testReadLineSkipsEmptySocketReadsAndDeliversFollowingFrame` - Asserts an empty-string socket read (a transient no-data return) is neither treated as EOF nor appended as a phantom chunk: readLine() skips it and delivers the frame from the following read.
- `testReadLineCoalescesPingFloodIntoSinglePongForNewestPing` - Asserts 18 pings coalesced with a data frame into one read cost EXACTLY one pong, carrying the NEWEST ping's payload (RFC 6455 5.5.3 permits eliding older answers, never the newest), while the data frame is still delivered.
- `testReadLineFlushesCloseEchoAfterCoalescedPingFlood` - Asserts the mandatory RFC 6455 5.5.1 Close echo occupies its own answer slot that a ping flood cannot displace: 17 pings plus a Close in one read yield exactly one pong for the newest ping followed by one Close echo mirroring the received status code.
- `testQueuedControlAnswerIsDroppedSilentlyOnceSocketReleased` - Asserts a control answer queued before the socket is released is dropped silently once close() nulls the socket: no write is attempted and the pending answer slot is cleared rather than lingering.
- `testReadLineIgnoresUnsolicitedPongWithoutAnswering` - Asserts an unsolicited server PONG (a legal unidirectional heartbeat under RFC 6455 5.5.3) is consumed silently with no answer frame written, and the data frame sharing the read is still delivered.
- `testReadLineDeliversSameReadDataBeforeCorruptCompressedFrameFailure` - Asserts a corrupt permessage-deflate FIN frame is deferred like every other terminal condition: the valid data frame from the SAME read is returned first and the typed ProtocolException ("inflate compressed WebSocket frame") surfaces on the following readLine() (#115).
- `testReadLineInflatesCompressedFragmentedMessage` - Asserts a permessage-deflate message split across an RSV1 first fragment and a final continuation reassembles the COMPRESSED bytes first and inflates once (RFC 7692), returning the original payload byte-identically.
- `testReadLineDeliversSameReadDataBeforeCorruptCompressedFragmentFailure` - Asserts a corrupt compressed FRAGMENTED message fails on its final continuation with the failure deferred (#115): the valid data frame from the same read is returned first and the inflate ProtocolException surfaces on the next readLine().
- `testConsumeSpanningFrameTopsUpSplitHeaderAndFoldsSurplusChunks` - White-box pin for consumeSpanningFrame()'s rescue paths: asserts a spilling frame whose header is split between the working buffer and the queued chunks is topped up until the header parses, that queued chunks reaching beyond the consumed frame are folded back rather than dropped (both the spanning payload and the trailing frame are delivered byte-exact), and that the spill state is fully retired afterwards (#164).
- `testDrainRejectsMaskedSpilledFrameAsProtocolViolation` - White-box pin for the spill path's own masked-frame guard: asserts a masked frame consumed by consumeSpanningFrame() fails the connection with ProtocolException ("masked (RFC 6455 5.1 violation)") instead of being silently unmasked and delivered.
- `testHandshakeRejectsMissingUpgradeHeaders` - Asserts a 101 response lacking the required "Upgrade: websocket" / "Connection: Upgrade" headers fails the handshake with a ConnectionException naming the missing headers and RFC 6455 4.1, instead of speaking WebSocket into a non-WebSocket stream.
- `testHandshakeRejectsWhenEitherUpgradeOrConnectionHeaderIsMissingAlone` - Asserts RFC 6455 4.1 requires BOTH headers: a 101 carrying only "Connection: Upgrade" and one carrying only "Upgrade: websocket" each fail the handshake, so neither flag defaults to accepted.
- `testHandshakeRejectsUnsolicitedCompression` - Asserts a server negotiating permessage-deflate that the client never offered fails the handshake with the extension named and the RFC 6455 4.1 citation, instead of silently flipping compression on.
- `testHandshakeValidatesCompressionParameters` - Asserts permessage-deflate negotiation rejects unknown parameters (naming the offender and the accepted set) and an acceptance that fails to echo server_no_context_takeover (RFC 7692 7.1.1.1), while a conformant response succeeds with compressionActive true.
- `testHandshakeAcceptsServerMaxWindowBitsWithinRange` - Asserts a volunteered server_max_window_bits in the inclusive 8..15 range is ACCEPTED in token, quoted-string, and upper-cased spellings (the raw 15-bit inflater decodes any smaller window), each handshake completing with compression active (RFC 7692 7.1.2.1).
- `testHandshakeAcceptsWindowBitsListedBeforeContextTakeoverEcho` - Asserts parameter order does not matter: an accepted server_max_window_bits continues the parameter loop so a server_no_context_takeover echoed after it is still seen and the negotiation completes.
- `testHandshakeRejectsOutOfRangeWindowBitsAndClientMaxWindowBits` - Asserts server_max_window_bits acceptance stays bounded to the RFC 7692 grammar: the value-less spelling, out-of-range and leading-zero values, prefix/suffix near-misses, and client_max_window_bits (never offered) each fail the handshake as unacceptable parameters.
- `testHandshakeRejectsNon101StatusLine` - Asserts a non-101 status line (403) fails the handshake with a ConnectionException quoting the server's verbatim status line, so operators see why the upgrade was refused.
- `testHandshakeRejectsStatusLineWithLeadingGarbage` - Asserts the 101 check is anchored at the START of the status line: a first line that merely contains "HTTP/1.1 101" further in is rejected (quoting it) rather than treated as a completed upgrade.
- `testHandshakeRejectsOversizedResponseHeaders` - Asserts a handshake response whose headers never terminate is bounded: past 16 KiB the client fails with a ConnectionException ("exceeded the maximum header size") instead of buffering indefinitely.
- `testHandshakeHeaderSizeBoundIsExclusiveAtExactly16384Bytes` - Asserts the 16 KiB header bound is EXCLUSIVE: a terminator-less response of exactly 16384 bytes fails with the peer-close message ("connection closed before response"), not the size guard.
- `testHandshakeParsesPaddedHeaderNamesAndUnpaddedValues` - Asserts header parsing tolerates real-world whitespace sloppiness: a name padded before the colon ("Upgrade : websocket") still matches and a value glued to the colon ("Connection:Upgrade") is read correctly, so the handshake completes.
- `testHandshakeRetainsSurplusBytesVerbatimWithoutParsingThemAsHeaders` - Asserts bytes after the header terminator (e.g. a pipelined NATS INFO frame) are retained byte-identically as the initial read buffer and never parsed as handshake headers, so a header-shaped surplus cannot affect the negotiation.
- `testWriteDeflatesPayloadOnlyWhenCompressionActive` - Asserts write() deflates exactly when permessage-deflate was negotiated: with compression active the emitted binary frame carries RSV1 and a payload that inflates back to the original bytes, and without negotiation the raw bytes go out unmarked (#61).
- `testHandshakeSkipsMalformedHeaderLineWithoutColon` - Asserts a colon-less junk header line in the response is skipped rather than fatal, with the RFC-required validation still running on the well-formed headers around it.
- `testHandshakeRejectsInvalidAcceptKey` - Asserts a Sec-WebSocket-Accept that does not match the SHA-1 of the client key plus GUID fails the connection ("invalid Sec-WebSocket-Accept"), per RFC 6455 4.1 step 4.
- `testHandshakeRejectsUnsupportedExtensionResponses` - Asserts that with compression offered, an extension response naming a different extension or a comma-separated multi-extension list fails the handshake quoting the server's response verbatim.
- `testHandshakeToleratesEmptyCompressionParameter` - Asserts empty permessage-deflate parameters (";;" from sloppy serialization) are skipped rather than treated as unacceptable, with the real parameters still validated and compression active afterwards.
- `testConnectEstablishesTlsForWssAndReportsTlsActive` - Against a self-signed local TLS server followed by the scripted HTTP upgrade, asserts wss:// establishes TLS during connect() and tlsActive() reports true (the success arm complementing testConnectCallsSetupTlsOnWssAndThrowsWhenServerIsPlainTcp).

## Integration Tests (`tests/Integration/`)

### tests/Integration/ClientParityIntegrationTest.php
- `testRespondHelperRepliesToRequester` - A subscriber's `respondWithHeaders('pong', ...)` replies to the requester's reply subject; asserts the reply payload is `pong` and the echoed `X-Echo` header equals the original `ping` payload.
- `testRequestManyCollectsMultipleReplies` - `requestMany` with maxResponses 3 collects three replies (`a`,`b`,`c`) emitted by a single responder into the requester's inbox and stops at the cap.
- `testMultiValueHeadersRoundTrip` - `publishWithHeaders` with a multi-value header round-trips through the server; `fromWireBlockMulti` returns `X-Tag` as `['one','two']` and `X-Single` as `['solo']`.
- `testConnectionLifecycleListenerObservesConnectAndClose` - A `connectionListener` records exactly `[ConnectionEvent::Connected, ConnectionEvent::Closed]` across connect then disconnect.
- `testDynamicTokenProviderAuthenticates` - A `tokenProvider` callback supplies the token on connect to the token-auth server; a pub/sub round trip succeeds and the provider is invoked at least once.
- `testPublishExpectationsEnforceLastSequence` - JetStream publish with a correct `expectedLastSequence` appends at seq+1, while a stale `expectedLastSequence` is rejected with `JetStreamException`.
- `testDeleteMessageRemovesStoredMessage` - `deleteMessage` removes a stored message by sequence; the message is retrievable before deletion and `getStreamMessage` throws `JetStreamException` afterward.
- `testAckSyncAndMessageMetadataOnPulledMessage` - `messageMetadata` on a pulled message exposes stream/consumer/streamSequence=1/numDelivered=1/numPending>=1/timestampNanos>0, and `ackSync` resolves once the server confirms the ack.
- `testPullConsumerStopHaltsLoop` - A pull consumer `handle` loop calling `stop()` from the handler after the second message halts the loop and returns a total of 2.
- `testUrlEmbeddedUserPasswordAuthenticates` - Credentials embedded as `user:pass@host` in the server URL authenticate against the userpass server; a pub/sub round trip returns `ok`.
- `testUrlEmbeddedTokenAuthenticates` - A token embedded as `token@host` in the server URL authenticates against the token server; asserts `serverInfo()` is non-null.
- `testInjectedTlsContextConnects` - An injected `ClientTlsContext` (CA + client cert) with `tlsHandshakeFirst` is used verbatim for the handshake; asserts `serverInfo()` is non-null (skips without TLS fixtures).
- `testConnectionAccessorsLive` - Verifies live connection accessors: `connectedUrl()` matches the URL, `maxPayload()`>0, `rtt()`>0.0, and `statistics()` reports outMsgs>=1 and inMsgs>=1 after a round trip.
- `testAuthenticationErrorFailsFast` - A bad token (with reconnect enabled, 5 attempts) raises `AuthenticationException` and, proven via the captured logger, logs zero "reconnect attempt" messages (fail-fast, no retry).
- `testBucketDiscovery` - `keyValueBucketNames()` and `objectStoreBucketNames()` list their respective created buckets, and KV discovery does not list the object-store bucket.
- `testKeyValueGetRevisionAndHistory` - `getRevision` reads the value (`red`) stored at the first revision, and `history('color')` returns all revisions oldest-first (`['red','green','blue']`).
- `testKeyValueMirrorBucketConfig` - A KV bucket created with `['mirror' => $primary]` produces a `KV_` stream whose config `mirror.name` points at the source bucket's `KV_` stream.
- `testKeyValueCompareAndDelete` - Compare-and-delete rejects a stale expected revision with `JetStreamException` (key still present) and succeeds with the current revision, leaving a `DEL` tombstone.
- `testTypedStreamAndConsumerBuilders` - Typed `StreamConfiguration` (workqueue retention, maxBytes) and `ConsumerConfiguration` (explicit ack, maxDeliver 3, ackWait) builders create assets whose fetched config matches the builder settings.
- `testStreamAndConsumerNames` - `streamNames()` (with and without a subject filter) lists the created stream and `consumerNames($stream)` returns exactly `[$consumer]`.
- `testGetLastMessageForSubjectLive` - `getLastMessageForSubject` returns the most recent message for a subject (`second-a`) with the matching subject across a wildcard stream.
- `testCreateOrUpdateStreamUpserts` - `createOrUpdateStream` creates the stream first then upserts it, updating the subject set from `['...one']` to `['...one','...two']` without an "already in use" error.
- `testKeyValueCreateKeyIsExclusive` - KV `createKey` succeeds first (seq>=1) but a second `createKey` on the live key throws `JetStreamException` carrying getCode() 400 and getErrCode() 10071 (#154); the value remains the first write.
- `testKeyValueKeysListsLiveKeys` - `keys()` returns live key names excluding a deleted key (`['alpha','gamma']`) and `listKeys()` returns the same result.
- `testKeyValueWatchOptionsReplayHistoryAndSignalCaughtUp` - `watch` with `ignoreDeletes` and an `onCaughtUp` callback replays last-per-subject history (delivers live `one='A'`, suppresses the deleted `two`) and fires onCaughtUp after the initial replay.
- `testObjectStoreUpdateMetaRenames` - `updateMeta` renames an object (`logo.bin`->`brand.bin`) with new metadata without re-uploading; the new name resolves with original bytes and the old name is tombstoned (`deleted` true).
- `testServiceHandlerRepliesWithCustomError` - A service endpoint that throws `ServiceError(429, 'Rate limited', body)` replies with `Nats-Service-Error`/`Nats-Service-Error-Code` headers and the error body as payload (retried until the SUB registers).
- `testObjectStoreTypedConfigApplied` - `ObjectStoreConfig(maxBytes, storage)` maps to the backing `OBJ_` stream whose config `max_bytes` equals the requested 2,000,000.
- `testObjectStoreDescriptionStored` - An object description passed to `put` is surfaced on the returned `ObjectInfo` and on a later `info()` lookup (`Project readme`).
- `testObjectStoreGetFollowsLink` - `get()` on a link object transparently follows it to the target's bytes (`the-payload`) and reports the target's name on the resolved info.
- `testObjectStoreAddLink` - `addLink` writes a resolvable link object (`isLink()` true) whose `info()` reports the target as `['bucket' => ..., 'name' => 'target.bin']`.
- `testObjectStoreSealRejectsWrites` - After `seal()` (returns true) a new `put` throws `JetStreamException` while pre-existing content remains readable.
- `testServiceGroupedMetadataAndCustomStats` - A grouped endpoint forwards its prefixed subject (`v1.work.<name>`) and per-endpoint metadata to `$SRV.INFO`, and a custom stats supplier's data (`queue_depth=3`) to `$SRV.STATS`.
- `testServiceDoneHandlerFiresWhenRunStops` - The service `onDone` handler fires when `run(0.3)` reaches its timeout window and stops.
- `testServiceDrainStopsServing` - A service endpoint serves a request before `drain()`, then after draining subsequent requests fail with a "No responders" `NatsException`.
- `testSubscriptionDrainStopsDelivery` - `drainSubscription` delivers the in-flight message (`one`) then removes the subscription so a later publish (`two`) is not delivered; asserts only `['one']` received.
- `testWebSocketCompressionAndCustomHeaders` - A WebSocket connection with permessage-deflate compression and a custom upgrade header round-trips a large compressible payload unchanged.
- `testWebSocketTransportCarriesPubSubAndJetStream` - The WebSocket transport carries a core pub/sub round trip (`ws-hello`) and a JetStream publish + read-back (seq>=1, payload matches) over the same ws:// connection.

### tests/Integration/HeartbeatSoakIntegrationTest.php
- `testIdlePublisherStaysAliveViaHeartbeatSelfRead` - With a 1s ping interval and maxPingsOut=2, an idle publisher-only client that never calls processIncoming() is left idle for ~4.5s; asserts the heartbeat timer self-reads its own PONGs so the connection stays Open with 0 reconnects (no false "server unresponsive" drop), and remains usable for a follow-up publish.
- `testConcurrentHeartbeatAndProcessIncomingDeliverAllMessages` - Runs an application processIncoming() loop concurrently with the 1s heartbeat self-read while publishing 10 messages over ~5s; asserts the readInProgress guard prevents overlapping reads (no PendingReadError/spurious reconnect), all 10 messages are delivered, the connection stays Open, and reconnects stay 0.

### tests/Integration/JetStreamIntegrationTest.php
- `testJetStreamAccountAndStreamLifecycle` - Verifies accountInfo reports a non-negative stream count and that createStream/getStream return the right name and deleteStream returns true against a live server.
- `testJetStreamConsumerAndPublishAck` - Creates a stream and durable consumer, publishes a message, and asserts the create/get consumer names, the publish ack's stream and seq>=1, and that consumer and stream deletions return true.
- `testJetStreamListConsumers` - Creates two durable consumers and asserts listConsumers returns names containing both.
- `testJetStreamUpdateStreamConfiguration` - Updates a stream to add a second subject and asserts both the updateStream result and a subsequent getStream contain both subjects.
- `testJetStreamPurgeStreamByFilter` - Publishes 2 messages on a purge subject and 1 on a keep subject, runs a filtered purge, and asserts only the 2 matching messages were purged, the kept message survives, and the purged subject's getLastMessageForSubject returns a >=400 JetStreamException.
- `testJetStreamGetStreamMessage` - Publishes one message and asserts getStreamMessage by the ack sequence returns the correct subject and payload.
- `testJetStreamGetStreamMessagePreservesZeroAndHeaders` - Asserts getStreamMessage preserves a literal "0" body and that a message published with headers exposes a non-null rawHeaders block decoding to the original custom header value.
- `testJetStreamDirectGetStreamMessage` - On an allow_direct stream, asserts directGetStreamMessage by sequence returns the body/subject, directGetLastMessageForSubject returns the newest payload, and a missing sequence raises a >=400 JetStreamException.
- `testConcurrentRequestsAllResolve` - Issues 12 concurrent $JS.API.INFO request/reply round-trips on one connection and asserts all 12 resolve within 5s with non-empty payloads (self-pumping read regression guard).
- `testJetStreamObjectStorePipelinedMultiChunkRoundTrip` - Puts a ~2KiB payload with a 64-byte chunk size (>16 chunks) and asserts get() returns exact bytes, proving pipelined multi-chunk upload preserved order and passed internal digest verification.
- `testJetStreamObjectStorePutStreamRoundTrip` - Uploads via putStream from a producer callback returning unaligned blocks and asserts the reported size matches, chunks>1, and get() returns the exact concatenated bytes (re-chunking round-trip).
- `testJetStreamListStreams` - Creates two streams and asserts listStreams returns names containing both.
- `testJetStreamScheduledPublish` - Schedules a publish (@at +2s) on an allow_msg_schedules stream and polls stream state until at least one message is delivered, asserting the ack stream and observed message count>=1.
- `testJetStreamScheduledPublishWithPerMessageTtl` - Schedules a publish with a per-message TTL on a stream enabling both allow_msg_schedules and allow_msg_ttl and asserts the ack stream and seq>=1.
- `testJetStreamScheduledPublishRejectsUnsupportedPatterns` - Asserts publishScheduled with a malformed schedule expression throws a JetStreamException containing "Unsupported schedule expression" (client-side rejection before any round-trip).
- `testJetStreamPullFetchAndAck` - Publishes a message, fetchNext on a pull consumer returns the payload with a non-null replyTo, then ack succeeds.
- `testJetStreamPullNakWithDelayRedelivery` - Fetches a message, NAKs with a 1.2s delay, polls (tolerating 408 timeouts) until the message is redelivered, and asserts the redelivered payload then acks it.
- `testJetStreamTermAndInProgressTokens` - Verifies a WPI/inProgress heartbeat delays redelivery (immediate fetch raises 404/408), then redelivery eventually arrives and is acked; and that TERM stops further redelivery (subsequent fetch raises 404/408).
- `testJetStreamPullIteratorBatching` - Publishes 5 messages and runs a pullConsumer iterator with batching=2, expires=700ms, iterations=4, acking each; asserts total returned is 5 and all 5 distinct payloads were seen.
- `testJetStreamInfiniteConsumeWithMaxBytesSurvivesOversizedPendingMessage` - Publishes an oversized head message; asserts a direct max_bytes-capped fetchBatch surfaces the 409 "Message Size Exceeds MaxBytes" status, and that an infinite consume loop with setMaxBytes() keeps (paced) pulling through that window, delivering a fitting message after the oversized one is deleted (#153).
- `testJetStreamPushConsumerHelperDelivery` - Subscribes a durable push consumer (default deliver subject), publishes one message, pumps processIncoming until received, and asserts the delivered NatsMessage payload.
- `testDrainDeliversJetStreamPushConsumerAckToTheServer` - (#150) a REAL JetStream push consumer whose handler calls the JetStream ack path (`ack()`) while the connection is Draining: the server's pushed message stays buffered on the socket (no pump before drain) so drain()'s flush-phase read delivers it during Draining; asserts the handler ran while Draining, the ack did not throw, the subscriber ends Closed, and the ack was recorded server-side (the consumer's `num_ack_pending` and `num_pending` both drain to 0, confirming durable acknowledgment rather than redelivery).
- `testJetStreamPushConsumerWithExplicitDeliverSubject` - Same as the durable push helper test but with an explicit deliver subject, asserting the delivered payload.
- `testJetStreamEphemeralPushConsumerDelivery` - Subscribes an ephemeral push consumer, publishes one message, pumps until received, and asserts the delivered payload (acking when a replyTo is present).
- `testJetStreamOrderedConsumerWithFilteredSubjectAfterPriorMessages` - Advances the stream with a non-matching message first, then publishes 5 matching messages interleaved with non-matching ones; asserts the ordered consumer delivers all 5 in order and de-duplicated (P0 stream-sequence gap-detection regression guard).
- `testJetStreamOrderedConsumerReplaysPreExistingBacklogInOrder` - Publishes an 8-message backlog (interleaved with non-matching subjects) before starting the ordered consumer and asserts the full matching backlog replays in order and de-duplicated.
- `testJetStreamOrderedConsumerRecoversFromDroppedDeliveryInOrder` - Uses DroppingTransport to drop exactly the second $JS.ACK-bearing delivery, then asserts exactly one frame was dropped yet all 5 messages arrive once in order, validating the recreate + by-sequence replay and stale-delivery fence.
- `testJetStreamOrderedConsumerWatchdogRecreatesReapedConsumer` - Deletes the ordered consumer server-side (standing in for an inactive_threshold reap / R1 restart) so the deliver inbox is silent, then asserts the idle-heartbeat watchdog recreates it and a message published afterwards is still delivered in order - a message that never arrives without the watchdog (#113).
- `testJetStreamKeyValueWatchWatchdogRecreatesReapedConsumerAndRecovers` - Deletes a KV watch's server-side consumer (standing in for an inactive_threshold reap) so its deliver inbox goes silent, then asserts the idle-heartbeat watchdog recreates a replacement consumer and the same watch handler still receives a value written after the reap, proving the lossless-watch contract rather than a dead watch that only reports an error (#113).
- `testJetStreamEphemeralPullConsumerFetchAndAck` - Creates an ephemeral consumer (asserting its streamName and non-empty name), publishes a message, fetchNext returns the payload, then acks.
- `testJetStreamKeyValueLifecycle` - Creates a KV bucket, watches "theme", puts "dark" and asserts the watch entry key/value; asserts get returns "dark"; then deletes and asserts get returns a null-value entry with operation "DEL".
- `testJetStreamKeyValueAdvancedParityOperations` - Exercises put/update (asserting seq>=2), getAll before purge (both keys present), purge (username gone, email remains), and getStatus reporting bucket, stream "KV_<bucket>", and messages>=1.
- `testJetStreamKeyValueKeysExcludeTombstonesAndMatchGetAll` - Behavior-preservation guard for #110: with a DEL and a PURGE tombstone present, keys()/listKeys() return exactly the non-deleted names, equal to array_keys(getAll()), and getAll() returns the stored 4KB values; protocol-agnostic, so it passes on both the pre-#110 per-key Direct Get path and the headers-only enumeration + batched getAll path.
- `testJetStreamObjectStoreListPreservedAcrossDeletedFilter` - Behavior-preservation guard for #110: with a deleted object present, list() returns the identical objects excluding the deleted one and list(includeDeleted:true) includes it; protocol-agnostic across the per-subject fan-out and batched multi_last Direct Get paths.
- `testJetStreamObjectStoreLifecycle` - Puts an object with metadata and asserts name/not-deleted, info metadata content-type, get payload, list count/name, delete sets deleted=true, get-after-delete returns null, and info-after-delete shows the tombstone deleted=true.
- `testJetStreamObjectStoreEmptyObjectRoundTrip` - Puts a 0-byte object asserting size=0 and chunks=0, then asserts get() returns '' within a 5s bound (no chunk-pull hang).
- `testJetStreamObjectStoreWatchDeliversUpdatesWithRevision` - Starts a watch, puts an object, pumps until seen, and asserts the delivered ObjectInfo name and a non-null revision>0.
- `testJetStreamObjectStoreWatchReplaysExistingObjectsWithSnapshotOption` - Puts two objects before watching with ObjectStoreWatchOptions (snapshot) and asserts both pre-existing objects are replayed to the watcher.
- `testJetStreamStreamPoliciesPersist` - Creates a stream with retention/storage/discard/max_msgs/max_bytes options and asserts each value persists in the fetched stream config.
- `testJetStreamPauseAndResumeConsumer` - Publishes a message, pauses the consumer (asserting paused=true and that a pull raises 404/408), then resumes (paused=false) and asserts fetchNext returns the message and acks it.
- `testJetStreamKeyValueHistoryAndTtlBehavior` - Creates a KV bucket with history=3 and a TTL, asserts the latest value and that config persists max_msgs_per_subject=3 and max_age, then polls until the key expires (get returns null).
- `testJetStreamKeyValueConcurrentWatchers` - Registers two watchers on the same key, puts v1 then v2, pumps until both observe both updates, and asserts each watcher saw v1 then v2 in order.
- `testJetStreamObjectStoreLargeObjectChunks` - Puts a >131072-byte payload spanning multiple chunks, asserts info is readable, and that get returns the exact payload with a matching digest.
- `testJetStreamObjectStoreDownloadCrossesBatchWindow` - Puts a ~9MiB payload yielding >64 chunks and asserts get reassembles the exact payload with a matching digest, exercising the multi-window pull loop.
- `testJetStreamObjectStoreDigestMismatch` - Tampers an object's metadata digest by publishing a corrupted meta record and asserts get() throws a JetStreamException containing "Object digest mismatch".
- `testJetStreamPushFlowControlAndHeartbeat` - Subscribes a push consumer with flow_control and idle_heartbeat, observes a control-frame window asserting no user payloads but >=1 frame processed, then publishes a message and asserts only that single user payload is delivered.
- `testJetStreamFetchBatchHandlesStatusFrames` - Publishes 2 messages, asserts fetchBatch(3) returns both (count>=2), acks them, purges the stream, and asserts a subsequent fetchBatch raises a 404/408 timeout JetStreamException.
- `testJetStreamAtomicBatchPublish` - On an allow_atomic stream, commits a 3-message atomic batch and asserts the batch ack reports batchCount=3 with a non-null batchId and the stream state shows 3 messages.
- `testJetStreamBatchedDirectGet` - On an allow_direct stream with 3 subjects, asserts directGetLastForSubjects returns 3 messages and directGetBatch over a sequence range returns all 3 with the expected payloads.

### tests/Integration/MultiConsumerIntegrationTest.php
- `testTwoDurableConsumersOnSameStreamEachReceiveAllMessages` - two independent durable consumers on one stream each receive the full message set in order (fan-out independence; one consumer acking does not consume the other's copy).
- `testSharedDurableConsumerLoadBalancesAcrossTwoConnectionsWithoutDuplication` - two client connections pulling the same durable consumer split the messages, delivering every message exactly once (load-balance, zero duplication).
- `testConsumersOnSeparateStreamsDoNotCrossTalk` - consumers on two streams with disjoint subjects each see only their own stream's messages (no cross-stream delivery).
- `testConcurrentOrderedConsumersOnSeparateStreamsStayInOrder` - two ordered consumers on separate streams over separate connections, pumped concurrently, each receive their stream complete and in order.
- `testCoreQueueGroupSubscribersLoadBalanceWithoutDuplication` - core NATS queue-group subscribers split messages, each delivered to exactly one member, with load distributed across members.

### tests/Integration/NatsCliInteropIntegrationTest.php
- `testKeyValueWrittenByThisClientIsReadableByNatsCli` - Creates a KV bucket and puts 'greeting' via this client, then runs `nats kv get --raw`; asserts the CLI exits 0 and reads back the exact value 'hello-from-php'.
- `testKeyValueWrittenByNatsCliIsReadableByThisClient` - Adds a KV bucket (`nats kv add --history=5`) and puts 'fromcli' via the CLI, then reads the key through this client; asserts the entry is non-null and its value equals 'hello-from-cli'.
- `testObjectStoreMetaWrittenByThisClientIsReadableByNatsCli` - Regression for #109: puts an object with default (empty) metadata via this client, then runs `nats object info`; asserts the CLI exits 0, output contains the object name 'doc.txt', and stderr does not contain "invalid" (empty meta no longer serializes as a rejected JSON array).
- `testObjectStoreWrittenByNatsCliIsReadableByThisClient` - Adds an object bucket and puts 'doc.txt' (content 'object-from-cli') via the CLI over stdin, then reads it through this client; asserts the object is non-null and its data equals 'object-from-cli'.

### tests/Integration/NatsClientIntegrationTest.php
- `testConnectAndDisconnect` - Connects to a live server, asserts `serverInfo()` is non-null, then disconnects cleanly.
- `testPublishAndSubscribeRoundTrip` - Subscribes to a random subject, publishes "hello", event-pumps `processIncoming` with a 2s cancellation, and asserts the received message payload is "hello".
- `testAutoUnsubscribeDeliversUpToMaxThenStops` - Receives one message, arms `unsubscribe($sid, 3)`, publishes three more, and asserts exactly m1-m3 reach the handler while m4 never arrives within a bounded settle window (auto-unsubscribe delivers the remaining allowance instead of discarding it, #112).
- `testRequestReply` - Sets up a server that replies "world" to a subject, pumps it concurrently, and asserts the client's `request()` returns "world" and the handler ran.
- `testPublishWithHeadersRoundTrip` - Publishes with custom headers via `publishWithHeaders`, asserts the subscriber's parsed wire headers contain X-Request-Id and Content-Type "text/plain" with payload "hello".
- `testRequestWithHeadersPropagatesHeaders` - Sends `requestWithHeaders` carrying X-Request-Id, asserts the responder saw that header value and the reply payload is "ok".
- `testNoEchoSuppressesSelfPublishedMessages` - With `noEcho: true`, subscribes then publishes on the same connection and asserts the handler never fires within an ~0.8s monotonic window.
- `testConnectWithServerRotationFallback` - Provides a dead first server URL plus a live one and asserts connect rotates to the live endpoint (non-null `serverInfo()`).
- `testServiceDiscoveryAndEndpoint` - Starts an "echo" service endpoint, requests it with retry on no-responders, asserts reply "reply:hello" and that the stats snapshot name is "echo".
- `testServiceStatsAndObserversWithHeaders` - Drives one schema-invalid and one valid request through a validated endpoint; asserts the invalid reply is a micro VALIDATION_ERROR with correlation id, valid reply echoes, stats show 2 requests/1 error/last_error/processing times, and observers emitted request_start/error/end with both correlation ids.
- `testServiceDiscoverySubjectsContract` - Starts a service with one schema and one plain endpoint, requests $SRV.PING/INFO/STATS/SCHEMA, and asserts each response's micro type, name, version/description, endpoint counts (2 each), and that only the schema endpoint exposes a `schema` field.
- `testServiceMultipleEndpoints` - Registers alpha and beta endpoints, requests both, asserts replies "alpha:one"/"beta:two" and that the stats snapshot lists 2 endpoints.
- `testServiceGroupedEndpointsHierarchy` - Builds nested groups (root -> v1/v2) with relative "echo" endpoints, requests the full hierarchical subjects, asserts replies "v1:hello"/"v2:hello" and that both computed subjects appear in stats.
- `testServiceConcurrentRequests` - Fires 8 concurrent requests at a non-blocking-delayed endpoint from 8 separate clients, asserts all replies "ok:<idx>" arrive and the endpoint stats report 8 requests.
- `testFragmentedFramesStillDispatch` - Using FakeTransport, feeds a MSG frame split across two reads and asserts the first `processIncoming` returns 0, the second returns 1, and the reassembled payload "hello" is delivered.
- `testSlowConsumerPolicyBehavior` - With `maxPendingMessagesPerSubscription: 1` and `SlowConsumerPolicy::Error`, feeds two queued messages via FakeTransport and asserts `processIncoming` throws ConnectionException "Subscription queue overflow".
- `testTlsHandshakeFirstConnection` - Connects to a TLS handshake-first fixture with CA/cert/key (skips if fixtures absent) and asserts non-null `serverInfo()`.
- `testStandardTlsUpgradeConnection` - Connects with `tlsHandshakeFirst: false` to a non-handshake-first TLS upgrade fixture (skips if fixtures absent) and asserts non-null `serverInfo()`.
- `testTlsConnectionFailsWithoutClientCertificate` - Connects to a TLS fixture requiring a client cert but supplies none, expecting a ConnectionException (skips if CA fixture absent).
- `testTlsConnectionFailsWithWrongCa` - Trusts the wrong CA file under strict verification, expecting connect to throw ConnectionException (skips if NATS_TLS_SKIP_VERIFY=1 or fixtures absent).
- `testTlsConnectionFailsWithPeerNameMismatch` - Sets `tlsPeerName` to a non-matching hostname under strict verification, expecting connect to throw ConnectionException (skips if NATS_TLS_SKIP_VERIFY=1 or fixtures absent).
- `testTokenAuthSuccessAndFailure` - Connects successfully with a valid token (non-null `serverInfo()`), then expects a ConnectionException when connecting with an invalid token.
- `testUserPasswordAuthSuccessAndFailure` - Connects successfully with valid username/password, then expects a ConnectionException when the password is wrong.
- `testJwtNonceAuthenticationFlow` - Signs the server nonce with the matching user seed (NkeySeedSigner) and asserts JWT auth connects with non-null `serverInfo()` (skips if JWT/seed fixtures absent).
- `testStandaloneNkeyAuthenticationFlow` - Uses NkeySeedSigner for standalone NKey challenge signing and asserts the connection succeeds with non-null `serverInfo()`.
- `testNoRespondersErrorSurface` - Requests a subject with no responder and asserts the thrown NatsException message contains "No responders" and the subject name.
- `testSeveredLiveConnectionMidIdleReconnectsAndResumesDelivery` - Uses SeveringTransport (`tests/Support/SeveringTransport.php`, a decorator over the real AmpSocketTransport) to force-close the live TCP socket mid-idle; asserts the client observes a genuine EOF, reconnects against the real server (`statistics()->reconnects >= 1`), replays the subscription, and a publish from a second independent client is delivered post-reconnect (#141).
- `testSeveredLiveConnectionMidTrafficRecoversAndDeliversNewTraffic` - Delivers one live message, then severs the real socket via SeveringTransport while a second client keeps publishing; asserts reconnect fires (`reconnects >= 1`) and NEW post-sever traffic reaches the resubscribed handler within a bounded monotonic deadline (messages between the sever and the SUB replay are lost by design) (#141).
- `testQueueGroupDistributesMessages` - Two workers in the same queue group drain concurrently while 40 messages are published; asserts all 40 are received exactly once total (no duplicates) and both workers received at least one.
- `testRequestTimeoutReturnsTimeoutError` - A responder receives but never replies; asserts `request()` with a 300ms timeout throws TimeoutException containing "Request timed out" and the subject, and the responder saw at least one message.
- `testDrainDuringInflightDelivery` - Publishes an in-flight message then calls `drain()`; asserts the in-flight message was delivered and a post-drain publish throws ConnectionException "Connection is not open".
- `testDrainDeliversHandlerAckToTheWire` - (#150) a push-consumer-style handler that ACKs by publishing to an ack subject (like a JetStream ack or `respond()`) while the connection is Draining; asserts the ack does not throw, reaches the wire (observed on a second connection), and the subscriber ends Closed.
- `testOversizedPublishIsRejected` - Publishes a payload one byte over the server's `max_payload` and asserts a ProtocolException containing "exceeds server max_payload".
- `testWildcardSubscriptionReceivesExpectedSubjects` - Subscribes to a single-token wildcard, publishes two matching and one deeper non-matching subject, and asserts only the two matching subjects/payloads ("a","b") are received.
- `testRequestCancellationStopsAwait` - Cancels an in-flight request (via external token after a 0.15s non-blocking delay) under a 30s timeout and asserts a CancelledException (not TimeoutException) is thrown before the deadline.
- `testServiceEndpointsLoadBalanceAcrossInstances` - Runs two identical service instances sharing the default queue group, fires 20 requests, and asserts both instances handled part of the load with a total in [requests, 2*requests) proving load-balancing rather than fan-out.
- `testFlushRoundTripConfirmsServerProcessing` - Subscribes, publishes, then calls `flush()` and asserts it resolves without error and the connection state remains Open (#66).
- `testSubscriptionQueuePollingDeliversLive` - Uses `subscribeQueue`, publishes q1/q2, sets a 3s timeout, and asserts polling `next()` returns payloads "q1" then "q2" (#66).
- `testIdleConnectionStaysOpenViaHeartbeat` - Stays fully idle for 3.5s (> maxPingsOut*pingInterval) and asserts the connection remains Open with zero reconnects, proving the heartbeat self-read consumes PONGs (#67).
- `testRequestTimeoutDoesNotPoisonConnection` - Forces a request timeout against a silent responder, then asserts the connection is still Open and a subsequent request to an echo responder succeeds with "pong:after-timeout" (#67).

## Behat Features (`features/`)

### features/auth/jwt_and_nkey_auth.feature
- Connect with JWT nonce authentication - connects using JWT nonce auth and asserts the authenticated connection succeeds.
- Connect with standalone NKey authentication - connects using a standalone NKey and asserts the authenticated connection succeeds.
- Connect with a generated credentials file - connects using a generated .creds credentials file and asserts the authenticated connection succeeds.

### features/auth/tls_auth.feature
- Connect with TLS handshake-first and client credentials - connects via TLS handshake-first with client credentials and asserts the authenticated connection succeeds.
- Reject a TLS client without a certificate - attempts a TLS connection lacking a client certificate and asserts the connection is rejected.

### features/auth/token_auth.feature
- Connect with the configured valid token - connects with the valid configured token and asserts the authenticated connection succeeds.
- Reject an invalid token - connects with an invalid token and asserts the connection is rejected.

### features/auth/userpass_auth.feature
- Connect with valid username and password - connects with valid user/password credentials and asserts the authenticated connection succeeds.
- Reject an invalid password - connects with an invalid password and asserts the connection is rejected.

### features/core/connection.feature
- Publish and subscribe with a single client - subscribes to a random subject, publishes "hello from behat", processes messages, and asserts that exact message is received.

### features/core/headers_queueing.feature
- Publish and request with headers while reading server info - with two clients, publishes/requests with custom headers and asserts the published message carries the custom headers, the request handler receives the custom request header, the reply is "ok", and server info is available.
- Queue group subscribers distribute messages without duplication - two workers share a queue group on one subject, publishes 20 messages, and asserts all 20 are distributed across workers with no duplicates.
- Polling subscription queue supports fetch, next, and fetchAll - creates a polling subscription queue, has the second client publish "one"/"two"/"three", fetches via fetch/next/fetchAll, and asserts those three values are returned.

### features/core/request_reply.feature
- Request and reply across two connected clients - second client replies "pong" on the request subject; first client requests "ping" and asserts the reply is "pong".

### features/jetstream-core/config_helpers.feature
- Republish forwards matching messages to the configured destination subject - creates a stream with republish from primary to secondary subject, publishes "republished-event" to primary, and asserts the secondary subscriber receives it on the secondary subject.
- Subject transform stores the message under the configured destination subject - creates a stream with a subject transform primary->secondary, publishes "transformed-event", fetches by last sequence, and asserts it is stored under the secondary subject with that payload.
- Source filtering replicates only matching origin messages - creates an origin stream and a sourced stream filtered to the primary subject, and asserts the sourced stream contains only "sourced-event" from the primary subject.
- Mirror replication copies origin messages without local subjects - creates an origin stream and a mirror stream from it, publishes "mirrored-event" to the origin subject, and asserts the mirror stream contains it.

### features/jetstream-core/consumer_helpers.feature
- Pull fetch and ACK returns the published payload - fetches and ACKs the next pull message "pull-event" and asserts the helper receives it.
- Delayed NAK redelivers a pull message - NAKs a pull message with delay then ACKs on redelivery, asserting the helper receives "redeliver-event".
- In-progress heartbeats delay redelivery and TERM stops later redelivery - exercises in-progress heartbeats and TERM on a pull consumer and asserts the helper receives "wpi-event".
- Durable push helper delivers a live message - subscribes with the durable push consumer helper, publishes "push-event", and asserts the helper receives it.
- Ephemeral pull helper fetches and ACKs a live message - creates an ephemeral pull consumer, fetches "ephemeral-event", and asserts the helper receives it.
- Ephemeral push helper delivers a live message - subscribes with the ephemeral push consumer helper, publishes "ephemeral-push-event", and asserts the helper receives it.
- Ordered consumer still delivers after a prior non-matching stream message - subscribes with the ordered consumer helper, publishes "ordered-event" after a non-matching message, and asserts the helper still receives "ordered-event".
- Pause and resume suppresses then restores pull delivery - pauses the consumer, verifies no delivery, resumes it, and asserts the helper then receives "paused-event".
- Fetch batch returns all requested messages - fetches a batch of 5 JetStream messages and ACKs them, asserting the batch contains 5 messages.
- Pull-consumer iteration processes messages across batched fetches - processes pull-consumer iteration for 5 messages in batches of 2 and asserts 5 messages are processed total.

### features/jetstream-core/management.feature
- Update a stream and inspect consumer and stream listings - creates a stream, updates it to add the secondary subject, creates a durable consumer, fetches its info, lists consumers and streams, and asserts both subjects are present, the consumer info matches, and both listings include the current consumer/stream.
- Direct get returns the last published stream message and purge clears the stream - creates a stream, publishes "direct-get-event", fetches by last sequence and asserts the direct get returns it; then purges the stream and asserts it has no stored messages.
- Typed stream and consumer configuration persist in JetStream - creates a stream and consumer with typed configuration and asserts the typed configuration persists on both the stream and the consumer.

### features/jetstream-core/stream_lifecycle.feature
- Fetch account info and manage a stream lifecycle - fetches JetStream account info, creates a stream, asserts the account info request succeeds and the stream is available, then deletes the stream and asserts it is removed.

### features/jetstream-data/key_value.feature
- Manage a KeyValue entry lifecycle - creates a bucket, watches key "theme", puts "theme"=dark and asserts the watch observes it and the entry reads "dark"; then deletes the entry and asserts it is marked deleted.
- Run advanced KeyValue parity operations - puts "username"=alice and updates to "bob", puts "email", fetches all entries and asserts both values; purges "username" and asserts it is absent while "email" remains; fetches status and asserts it references the current bucket.

### features/jetstream-data/object_store.feature
- Manage an Object Store object lifecycle - creates a bucket, watches metadata, stores "logo.txt" (content "hello-object", type text/plain) and asserts the watch observes it, info shows the content type, download and callback streaming both return "hello-object", listing includes the object, status references the bucket, and after deletion the object is marked deleted.

### features/jetstream-data/scheduled_publish.feature
- Publish a delayed message through the scheduler - creates a stream with scheduling enabled, publishes the scheduled message "scheduled-event", and asserts the scheduled publish is acknowledged for the stream and the message becomes visible in the stream.

### features/resilience/client_resilience.feature
- no_echo suppresses self-published messages - connects with no_echo enabled, subscribes, publishes "self" from that client, and asserts the client does not receive its own message.
- Request without responders surfaces a no responders error - requests on a subject with no responders and asserts the request fails with a no-responders error.
- Request timeout surfaces a timeout error after a responder receives the request - with a silent responder subscribed, requests and waits for timeout, asserting the request fails with a timeout error and the silent responder did receive the request.
- Drain flushes in-flight delivery before closing the connection - drains a subscriber after publishing an in-flight message and asserts draining flushes the in-flight message and closes the client.
- Wildcard subscriptions only receive matching subjects - subscribes to a wildcard pattern and publishes matching and non-matching subjects, asserting only the matching wildcard subjects and payloads are received.
- Oversized publish is rejected before writing to the server - publishes a payload larger than the server max payload and asserts the oversized publish is rejected client-side.
- Auto-unsubscribe delivers remaining messages up to the maximum then stops - subscribes, arms unsubscribe with a maximum of 2 messages, publishes 3 messages, and asserts exactly 2 are delivered with none arriving after the maximum (#112).

### features/services/grouped_endpoints.feature
- Dispatch requests across grouped echo endpoints - starts a grouped echo service, requests both grouped endpoints with payload "hello", and asserts the replies are "v1:hello" and "v2:hello" and the service stats list both grouped subjects.

### features/services/service_discovery.feature
- Start an echo service and reply to requests - starts an echo service, requests "hello", and asserts the reply is "reply:hello".
- Expose discovery payloads for schema and plain endpoints - starts a discovery service with schema and plain endpoints, queries discovery subjects, and asserts the ping response describes the service, info and stats each list 2 endpoints, and the schema response includes schema only for the schema endpoint.
- Validate requests and emit observer correlation metadata - starts a validated service with observers, sends invalid and valid requests, and asserts the invalid response is a validation error, the valid response echoes the request, stats record 2 requests and 1 error, and observers capture both correlation ids.
