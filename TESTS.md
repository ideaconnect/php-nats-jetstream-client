# Test Catalog

Every automated test in the suite with a one-line description of what it verifies. When you add or change a test, update the matching entry here. The per-file sections below are authoritative; the counts are indicative.

## How to run

- **Unit** (no server): `composer test:unit` - protocol encode/parse, connection state/handshake/reconnect, subscriptions & backpressure, and JetStream/KV/ObjectStore/Services logic exercised against in-memory transport doubles (`tests/Support`).
- **Integration** (live server): `RUN_INTEGRATION=1 composer test:integration`, or `composer test:e2e` for the full Dockerised stack (TLS/auth/WebSocket variants). Real connect/auth/TLS/WebSocket, JetStream/KV/ObjectStore/Services round-trips, reconnect, heartbeat soak, multi-consumer concurrency, and `nats` CLI interop.
- **Behat** (live server): `composer test:bdd` - behaviour specs.

Indicative totals: ~860 unit tests, ~131 integration tests, ~46 Behat scenarios.

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
- `testUpgradeTlsThrowsWhenConnectedWithoutTlsContext` - With a held-open plaintext connection, asserts upgradeTls() fails fast with TlsRequiredException ("TLS upgrade requested but no TLS context") rather than leaving the socket plaintext.

### tests/Unit/BasicJsonSchemaValidatorTest.php
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
- `testCommitSendsBatchHeadersAndParsesAck` - Commits a 3-message batch and asserts the parsed ack (batchCount=3, batchId="batch-xyz"), that exactly 3 writes carry `Nats-Batch-Id:batch-xyz`, the START (seq 1) is a request with no commit marker, the intermediate (seq 2) is fire-and-forget, and the commit (seq 3) is a request carrying `Nats-Batch-Commit:1`.
- `testCommitCoalescesIntermediatesIntoSingleWrite` - A 5-message commit performs exactly 3 batch transport writes: the start request, ONE coalesced write whose bytes are the exact concatenation of the intermediate HPUB frames (sequences 2-4, no reply inbox, staging order), and the commit request - instead of one awaited write per intermediate (#138).
- `testOversizedIntermediateThrowsBeforeAnyIntermediateWrite` - With the server advertising `max_payload:256`, an oversized intermediate (300-byte payload + batch headers) throws `ProtocolException` ("exceeds server max_payload") before ANY intermediate reaches the wire - only the start frame is written, no seq-2 frame and no commit marker appear in any write (#138).
- `testCommitRejectedAtStart` - When the batch-start request gets an error JSON reply, `commit()` throws `JetStreamException` ("atomic publish not enabled") and no commit marker nor seq-3 message is written (publish aborts at start).
- `testCommitAbortsWhenBatchStartIsAcknowledgedAsPlainPublish` - A batch START acknowledged with a normal PubAck (pre-2.12 server storing it as a plain publish) aborts with UnsupportedFeatureException (feature allow_atomic, server version reported) before any further batch message is published (#130).
- `testCommitRejectsMultiMessageAckWithoutBatchFields` - A multi-message commit acknowledged by a PubAck without batch id/count (nothing was committed as a batch) throws UnsupportedFeatureException instead of reporting success (#130).
- `testSingleMessageBatchAcceptsPlainPubAck` - A single-message batch (trivially atomic) accepts a plain PubAck commit ack without batch fields (#130).
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
- `testFromFileReadsValidCredsFile` - Asserts `fromFile()` reads a valid creds file written to a temp file, returning the expected jwt and nkeySeed values (cleaning up the temp file afterward).

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
- `testUnpinConsumer` - unpinConsumer() issues a CONSUMER.UNPIN request carrying the group and returns true.
- `testPinIdOf` - pinIdOf() extracts the Nats-Pin-Id header value, returning null when absent.
- `testDirectGetBatchCollectsUntilEob` - directGetBatch() collects multiple HMSG replies, stops at the 204 EOB, and does not consume a frame sent after EOB.
- `testDirectGetLastForSubjects` - directGetLastForSubjects() sends multi_last with batch sized to the subject count and terminates on Nats-Num-Pending: 0.
- `testDirectGetBatchSurfacesError` - directGetBatch() surfaces a 408 status frame as a JetStreamException with code 408.
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
- `testKeyValueRejectsInvalidBucketName` - keyValue() rejects a dotted bucket name ("Invalid bucket name").
- `testObjectStoreRejectsInvalidBucketName` - objectStore() rejects a slashed bucket name ("Invalid bucket name").
- `testExtractSequencesParseElevenTokenDomainReplySubject` - (reflection) extractStreamSequence() parses the stream sequence from an 11-token domain $JS.ACK subject without a trailing random token.
- `testExtractStreamSequenceReturnsNullForInvalidReplySubject` - (reflection) extractStreamSequence() returns null for no-reply, too-short, wrong-prefix, and non-integer-sequence subjects.
- `testHandlePushControlMessageReturnsFalseForNonControlStatus` - (reflection) handlePushControlMessage() returns false for a non-control 404 status message.
- `testHandlePushControlMessageHeartbeatWithoutReplyReturnsTrue` - (reflection) handlePushControlMessage() returns true for a status-100 heartbeat lacking a reply subject.
- `testHandlePushControlMessageRepliesToJetStreamFlowControlSubject` - (reflection) handlePushControlMessage() returns true and PUBs an empty reply to the $JS.FC flow-control reply subject.
- `testGetStreamMessagePreservesZeroPayload` - getStreamMessage() preserves a falsy "0" body and leaves rawHeaders null.
- `testGetStreamMessageDecodesHeaders` - getStreamMessage() base64-decodes the stored hdrs block onto rawHeaders and the body onto payload.
- `testGetStreamMessageWithoutHeadersReturnsNullRawHeaders` - getStreamMessage() leaves rawHeaders null when no header block is stored.
- `testDirectGetStreamMessageReturnsRawBodyAndHeaders` - directGetStreamMessage() returns the original subject (Nats-Subject), raw body, and decoded headers via the DIRECT.GET endpoint.
- `testDirectGetLastMessageForSubjectRequestsLastBySubj` - directGetLastMessageForSubject() requests DIRECT.GET with last_by_subj and returns subject/payload.
- `testDirectGetStreamMessageThrowsOnNotFound` - directGetStreamMessage() throws JetStreamException ("Message Not Found") on a 404 status reply.
- `testSubscribeOrderedConsumerRecreatesOnSequenceGap` - subscribeOrderedConsumer() discards an out-of-order delivery, deletes the old consumer once, recreates exactly once from opt_start_seq (last in-order+1), and delivers only the in-order message.
- `testSubscribeOrderedConsumerIgnoresStaleDeliveryFromPreviousConsumerInstance` - a stale delivery from a different consumer instance name is ignored (no recreate) even when its consumer sequence matches the expected next.
- `testSubscribeOrderedConsumerRecreatesOnHeartbeatTailGap` - an idle heartbeat reporting Nats-Last-Consumer ahead of what was processed triggers exactly one recreate from the last in-order point (opt_start_seq).
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
- `testSubscribeOrderedConsumerDeliversMessageWithoutAckMetadata` - subscribeOrderedConsumer() best-effort delivers a message that has no $JS.ACK reply (no ordering metadata).
- `testSubscribeOrderedConsumerToleratesDeleteConsumerFailure` - subscribeOrderedConsumer() absorbs a deleteConsumer error during gap recovery, still recreates, and delivers only the in-order message.
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
- `testDirectGetBatchReturnsEmptyArrayOnTimeout` - directGetBatch() returns an empty array (catching the CancelledException) when the wait-cancellation fires on a blocking idle socket.
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

### tests/Unit/JsMessageMetadataTest.php
- `testFromMessageReturnsNullWhenReplyToIsNull` - `fromMessage()` returns null when the message has no reply subject.
- `testFromMessageReturnsNullWhenFirstTokenIsNotJs` - `fromMessage()` returns null when the first reply-subject token is not "$JS".
- `testFromMessageReturnsNullWhenSecondTokenIsNotAck` - `fromMessage()` returns null when the second token is not "ACK" (e.g. "NAK").
- `testFromMessageReturnsNullForUnrecognisedTokenCount` - `fromMessage()` returns null for a reply subject whose token count is not 9, 11, or 12 (7 tokens hits the default match branch).
- `testFromMessageParses9TokenForm` - Parses a canonical 9-token `$JS.ACK` subject and asserts stream, consumer, numDelivered=3, streamSequence=42, consumerSequence=7, timestampNanos, numPending=5, and domain=null.
- `testFromMessageParses11TokenFormWithRealDomain` - Parses the 11-token domain-prefixed subject and asserts domain="hub" plus all numeric/stream/consumer fields including numPending=0.
- `testFromMessageNormalizesUnderscoreDomainToNull` - When the domain token is "_" (server placeholder), the parsed `domain` is normalized to null.
- `testFromMessageParses12TokenForm` - Parses the 12-token form (domain + trailing random token), silently ignoring the 12th token while all other fields parse correctly.
- `testTimestampReturnsCorrectUtcDatetime` - `timestamp()` converts a nanosecond epoch to a `DateTimeImmutable` with zero UTC offset and the expected "2023-11-14 22:13:20" date/time.
- `testTimestampPreservesMicrosecondPrecision` - `timestamp()` preserves sub-second precision, formatting 500_000 ns as microseconds "000500".
- `testTimestampHandlesZeroNanoseconds` - `timestamp()` returns a `DateTimeImmutable` of "1970-01-01 00:00:00" for a zero-nanosecond value.

### tests/Unit/KeyValueBucketTest.php

- `testGetFallsBackToStreamMessageWhenDirectGetUnavailable` - when Direct Get returns 503 (no-responders), get() falls back to STREAM.MSG.GET and still returns the value (operation PUT, revision 9), emitting both DIRECT.GET and STREAM.MSG.GET requests.
- `testBucketCreateAndDelete` - create() and deleteBucket() map to STREAM.CREATE.KV_cfg and STREAM.DELETE.KV_cfg, returning the created stream name and a true delete result.
- `testBucketCreateSendsKvDefaultsAndAllowsOverride` - create() defaults include deny_delete:true and discard:new (ADR-8/nats.go parity) and an explicit user override (deny_delete:false, discard:old) wins (#132).
- `testPutGetDelete` - put/get/delete round-trip parses values correctly and uses the right subjects (PUB for put, DIRECT.GET for get, HPUB with KV-Operation:DEL for delete).
- `testCreateKeySucceedsWhenAbsent` - createKey() on an absent key publishes with Nats-Expected-Last-Subject-Sequence:0 and returns the ack (#19).
- `testCreateKeyThrowsWhenKeyExists` - createKey() throws JetStreamException "Key already exists" when the wrong-last-sequence ack is followed by a get() showing a live value (#19).
- `testCreateWithMirrorTranslatesBucketName` - create() with a mirror translates the mirror bucket name to KV_src, emits an empty subjects list, and targets STREAM.CREATE.KV_dst (#62).
- `testCreateWithSourcesAndExtendedConfig` - create() with sources translates each source name to KV_b1/KV_b2 and passes through compression and placement config (#62).
- `testGetRevisionReturnsEntryAtSequence` - getRevision() reads a specific sequence via STREAM.MSG.GET (with "seq":2) and returns the entry at revision 2 (#33).
- `testGetRevisionReturnsNullForDifferentKey` - getRevision() returns null when the message at that sequence belongs to a different key (#33).
- `testDeleteWithExpectedRevision` - delete() with an expected revision emits the KV-Operation:DEL marker plus Nats-Expected-Last-Subject-Sequence:4 compare-and-delete header (#34).
- `testHistoryReturnsEmptyWhenNoPending` - history() returns an empty array when the consumer reports num_pending:0 (#41).
- `testHistoryCollectsAllRevisions` - history() collects all delivered revisions in order (values v1,v2; revisions 5,6), stopping when pending reaches zero (#41).
- `testHistoryToleratesDeliveryWithoutMetadataAndKeepsCollecting` - history() skips a metadata-less delivery (no $JS.ACK reply subject) without throwing out of the dispatch loop and records only the valid revision (#96).
- `testKeysReturnsLiveKeyNames` - keys() returns only live key names, excluding keys whose latest record is a DEL tombstone (#25).
- `testWatchOptionsConfigureConsumer` - watch() with includeHistory/metaOnly/ignoreDeletes drives deliver_policy:all and headers_only:true on the consumer config (#26).
- `testWatchResumeFromRevisionUsesStartSequence` - watch() with resumeFromRevision:42 configures deliver_policy:by_start_sequence and opt_start_seq:42 (#26).
- `testGetMissingReturnsNull` - get() returns null when Direct Get replies with a 404 not-found status.
- `testInvalidKeyRejected` - put() with a key containing a space throws JetStreamException "Invalid KV key".
- `testUpdateWithExpectedRevision` - update() sends the optimistic Nats-Expected-Last-Subject-Sequence:2 header and returns the new ack sequence.
- `testPurge` - purge() emits KV-Operation:PURGE and Nats-Rollup:sub headers and returns the ack sequence.
- `testPutWithTtl` - put() with a per-key ttl emits the Nats-TTL:60s header (#4).
- `testDeleteWithTombstoneTtl` - delete() with a tombstoneTtl emits Nats-TTL:120s alongside the KV-Operation:DEL marker (#4).
- `testGetStatus` - getStatus() maps stream state counters to bucket/stream/messages/bytes fields.
- `testGetAll` - getAll() returns only the latest non-deleted values per key, skipping a PURGE-marked key, using concurrent Direct Get requests.
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
- `testWatchUpdatesOnlyUsesNewDeliverPolicy` - watch() with updatesOnly:true uses deliver_policy:new.
- `testWatchWithDefaultOptionsUsesLastPerSubject` - watch() with a default KeyWatchOptions() instance uses deliver_policy:last_per_subject.
- `testWatchWithOnCaughtUpToleratesMessageWithoutMetadata` - watch() with an onCaughtUp callback does not throw when a delivery lacks $JS.ACK metadata; the entry still reaches the handler and caughtUp stays false since it can't be determined (#90).
- `testWatchFiresOnCaughtUpImmediatelyOnEmptyBucket` - watch() on an empty bucket (num_pending:0) fires the onCaughtUp signal immediately from the created consumer's num_pending, without any delivery (#99).

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
- `testCleanupRequestSubscriptionFallsBackToLocalDropWhenClosed` - asserts that when state is Closed, `cleanupRequestSubscription` drops the subscription, meta, and pending-message entries locally.
- `testRecoverConnectionDisabledThrowsImmediately` - asserts `recoverConnection` throws `ConnectionException` "Reconnect is disabled" when reconnect is disabled.
- `testRecoverConnectionExhaustedSetsClosedState` - asserts that with all connect attempts failing, `recoverConnection` throws `ConnectionException` "Reconnect attempts exhausted" and leaves the connection in Closed state.
- `testConnectReturnsImmediatelyWhenAlreadyOpen` - asserts `connect()` is a no-op (no transport connect calls, stays Open) when the connection is already Open.
- `testAwaitServerInfoThrowsWhenInfoNeverArrives` - asserts `awaitServerInfo` throws `ConnectionException` "Expected INFO during connect" when no INFO line ever arrives.
- `testAwaitInitialPongThrowsWhenPongNeverArrives` - asserts `awaitInitialPong` throws `ConnectionException` "Expected PONG after CONNECT" when no PONG ever arrives.
- `testAwaitInitialPongHandlesParsedControlFrames` - asserts `awaitInitialPong` handles parsed control frames in a combined buffer and responds to PING with a `PONG\r\n` write.
- `testAwaitInitialPongThrowsOnParsedErrFrame` - asserts `awaitInitialPong` throws `ConnectionException` "Server error during connect" on a parsed `-ERR` frame.
- `testAwaitServerInfoAllowsMoreThanEightPollsBeforeInfoArrives` - asserts `awaitServerInfo` keeps polling past 8 empty reads and parses the INFO once it arrives (serverId `S9`).
- `testAwaitInitialPongAllowsMoreThanEightPollsBeforePongArrives` - asserts `awaitInitialPong` keeps polling past 8 `+OK` reads and returns null once PONG arrives.
- `testAwaitServerInfoRespondsToPingBeforeInfo` - asserts `awaitServerInfo` replies to a PING received before INFO with `PONG\r\n` and then parses the INFO (serverId `S4`).
- `testAwaitServerInfoParsesInfoLine` - asserts `awaitServerInfo` parses a raw INFO line into a `ServerInfo` with serverId `S1`.
- `testAwaitServerInfoParsesInfoFrame` - asserts `awaitServerInfo` parses an INFO frame into a `ServerInfo` with serverId `S2`.
- `testAwaitInitialPongThrowsOnErrLine` - asserts `awaitInitialPong` throws `ConnectionException` "Server error during connect" on a `-ERR Permissions Violation` line.
- `testHandleFramePongResetsOutstandingPingAndDrainFlag` - asserts a Pong frame in `handleFrame` resets `outstandingPings` to 0 and clears the `drainFlushPending` flag.
- `testHandleFrameErrThrowsConnectionException` - asserts an Err frame in `handleFrame` throws `ConnectionException` "Server sent error frame".
- `testHandleFrameInfoUpdatesServerInfo` - asserts an Info frame in `handleFrame` updates `serverInfo()` (serverId `S2`, maxPayload 2048).
- `testHandleFrameRecoverableErrDoesNotThrow` - asserts a recoverable "Permissions Violation for Publish" Err frame does not throw and leaves the state Open.
- `testHandleFrameIgnoresUnknownSubscriptionSid` - asserts a Msg frame for an unknown sid is ignored and no pending messages are buffered.
- `testDrainAllPendingDeliversBufferedMessagesInOrder` - asserts `drainAllPending` delivers buffered messages to the subscription callback in FIFO order (`['a','b']`).
- `testEnforceMaxPayloadAllowsUnknownServerInfoAndThrowsWhenExceeded` - asserts `enforceMaxPayload` is a no-op without server info, then throws `ProtocolException` "exceeds server max_payload" once a max_payload-8 ServerInfo is set and 9 bytes are checked.
- `testCleanupRequestSubscriptionNoOpForUnknownSid` - asserts `cleanupRequestSubscription` is a no-op for an unknown sid (all subscription maps remain empty).
- `testCleanupRequestSubscriptionFallsBackWhenUnsubscribeThrows` - asserts that when the UNSUB write throws, `cleanupRequestSubscription` still drops the subscription, meta, and pending-message entries locally.
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
- `testAutoUnsubscribeOnNonOpenConnectionDefersArmingInsteadOfDestroyingSubscription` - unsubscribe(sid, max) while state is not Open (mid-reconnect) neither throws nor destroys the subscription; the max is armed so recovery re-arms it via resubscribeAll (#112).
- `testAutoUnsubscribeArmWriteFailureKeepsSubscriptionArmedForRecovery` - a failed arming UNSUB write propagates but keeps both the subscription and the armed max, so the next recovery can re-arm it (#112/#116).
- `testPlainUnsubscribeOnClosedConnectionDropsStateWithoutThrowing` - plain unsubscribe(sid) for a known sid on a Closed connection releases local state silently instead of throwing (#116).
- `testAutoUnsubscribeCleansUpEvenWhenHandlerThrowsOnMaxDelivery` - a handler that throws on its final (max-th) delivery still leaves the subscription torn down; the terminal cleanup runs in a finally rather than after the handler returns (#112).
- `testSubscribeWithQueueGroupSendsSubFrameAndDeliversToHandler` - subscribe() with a queue group emits "SUB tasks.process workers 1" and dispatches a matching MSG to the handler.
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
- `testReconnectExhaustionReportsAbandonedBufferedPublishes` - Exhausting reconnect attempts with a non-empty reconnect buffer surfaces the abandonment through the errorListener and clears the buffer so a later manual connect() cannot replay a dead epoch (#123).
- `testPublishDuringRecoverySocketCloseBuffersInsteadOfRacingDeadSocket` - A publish landing while recovery is closing the dead socket takes the reconnect buffer (state flips off Open before recovery's first await) and reaches the wire only after the new handshake (#124).
- `testProcessIncomingDispatchesHmsgWithRawHeaders` - An HMSG frame is delivered with rawHeaders and payload kept separate.
- `testProcessIncomingRespondsToServerPing` - A server PING is answered with a protocol PONG (1 frame processed).
- `testSlowConsumerDropOldestPolicy` - With maxPending=1 and DropOldest, only the newest message ("second") is delivered.
- `testSlowConsumerDropNewestPolicy` - With maxPending=1 and DropNewest, only the earliest message ("first") is delivered.
- `testSlowConsumerErrorPolicyThrows` - With maxPending=1 and Error policy, an overflow throws ConnectionException ("Subscription queue overflow").
- `testSlowConsumerErrorOnOneSidDoesNotDiscardSiblingFrames` - An Error-policy overflow on one sid no longer discards same-chunk frames for other sids: siblings are delivered, then the overflow surfaces (#128).
- `testServerPingWithFailingPongWriteStillDeliversCoChunkedMessages` - A failing PONG reply to a server PING does not discard same-chunk MSG frames; the write failure surfaces after dispatch (#128).
- `testHeartbeatReadSurfacesFatalErrAndDeliversCoChunkedMessages` - A fatal -ERR consumed by the heartbeat self-read is no longer swallowed: co-chunked messages are delivered and the error surfaces via the errorListener (#128).
- `testRequestReturnsFirstReplyMessage` - request() returns the first reply payload and emits SUB inbox, PUB with reply subject, then UNSUB.
- `testRequestReturnsReplyDeliveredOnSameTickAsTimeout` - A reply delivered in the same processIncoming() tick the deadline fires is returned rather than discarded as a timeout (completion-vs-timeout race).
- `testRequestTimesOutWithoutReply` - request() raises TimeoutException ("Request timed out") when no reply arrives and still emits UNSUB.
- `testRequestManyCollectsUpToMaxResponses` - requestMany() collects replies up to maxResponses (A,B,C) and emits PUB then UNSUB (#21).
- `testRequestManyStopsOnStallInterval` - requestMany() with no maxResponses stops after the per-message stall interval, returning the 2 received replies (#21).
- `testRequestManyReturnsEmptyOnNoResponders` - requestMany() returns an empty array on a 503 no-responders status sentinel (#21).
- `testDrainStopsWhenHandlerUnsubscribesItself` - A handler that unsubscribes itself after the first message stops further delivery (only "first" delivered).
- `testServerPoolPreservesOrderWithoutRandomize` - With randomizeServers off, the first dial targets the first configured server (#55).
- `testRandomizeServersDialsFromPool` - With randomizeServers on, the first dial still targets one of the configured pool members (#55).
- `testRetryOnFailedInitialConnectSucceedsAfterRetry` - retryOnFailedInitialConnect retries the first connect (reconnect disabled) until Open, with >= 2 connect attempts (#56).
- `testFailedInitialConnectThrowsWithoutRetryOption` - A failed first connect throws ConnectionException when both retryOnFailedInitialConnect and reconnect are off (#56).
- `testTerminalInitialConnectFailureClosesTransportSocket` - A terminal initial-connect failure (reconnect and initial-retry off) closes the transport socket best-effort instead of leaving the dialed fd open until GC (#133).
- `testReconnectExhaustionClosesLastAttemptSocket` - Exhausted recovery closes the LAST attempt's socket (attempts + 1 close calls in total): per-attempt closes happen only at each attempt's start, so the terminal exit must add the final close (#133).
- `testConnectExtractsUrlCredentials` - Credentials in the server URL are stripped from the dialed DSN and applied as user/pass in the CONNECT payload (#37).
- `testRequestUsesConfiguredInboxPrefix` - request() uses the configured inbox prefix for both the SUB and the PUB reply subject.
- `testRequestRejectsNonPositiveTimeout` - request() with timeout 0 throws TimeoutException ("Request timeout must be greater than zero").
- `testRequestCanBeCancelledAndCleansUpSubscription` - request() with a pre-cancelled token throws CancelledException and still emits UNSUB.
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
- `testRequestTimeoutPreservesOriginalExceptionDuringCleanup` - A request() timeout surfaces TimeoutException ("Request timed out") even though cleanup emits UNSUB.
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

### tests/Unit/NatsHeadersTest.php
- `testToWireBlockEmitsRepeatedLinesForListValue` - Asserts a list-valued header (`Link` => [a.txt, b.txt]) encodes to one `Link:...\r\n` line per element plus the scalar `Nats-Msg-Id:1\r\n` line (multimap encoding, #42).
- `testFromWireBlockMultiPreservesAllValues` - Asserts `fromWireBlockMulti` returns all repeated values as a list (`Link`=>[a.txt,b.txt], scalar wrapped as [one]) while `fromWireBlock` is last-value-wins (`Link`=>b.txt).
- `testFromWireBlockMultiParsesStatusLine` - Asserts a `NATS/1.0 503 No Responders` status line parses into single-element lists `Status`=>['503'] and `Description`=>['No Responders'].
- `testRoundTripWireEncoding` - Asserts `toWireBlock` output starts with `NATS/1.0\r\n`, ends with `\r\n\r\n`, and round-trips back through `fromWireBlock` to the original header map.
- `testFromWireBlockSkipsMalformedHeaderLines` - Asserts `fromWireBlock` returns [] for null/empty input and skips lines with no separator or empty name, keeping only the valid `Valid` header.
- `testFromWireBlockParsesStatusLine` - Asserts a `NATS/1.0 100 Idle Heartbeat` block parses into `Status`, `Description`, and the following `Nats-Consumer-Stalled` header.
- `testToWireBlockRejectsEmptyHeaderName` - Asserts `toWireBlock` throws InvalidArgumentException ('Header name') for an empty-string header name.
- `testToWireBlockRejectsHeaderNameWithColonOrWhitespace` - Asserts `toWireBlock` throws InvalidArgumentException ('Header name') for a name containing a colon (`a:b`).
- `testHeaderValueSurroundingWhitespaceRoundTripsSymmetrically` - Asserts a value with leading/trailing spaces round-trips to its trimmed form (`'  spaced  '` => `'spaced'`), so encode and decode trim symmetrically.
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
- `testAllowsDisabledHeartbeatAndEmptyServers` - Asserts that `pingIntervalSeconds: 0`, `maxPingsOut: 0`, and an empty servers list are all accepted (heartbeat-disabled is legitimate), preserving the zero values.
- `testDefaultsMatchDocumentedValues` - Asserts the full set of constructor defaults (servers, name, inboxPrefix, all timeouts, reconnect params, ping/TLS/auth fields, slowConsumerPolicy=DropOldest, reconnectBufferSize=8388608, WebSocket/logger defaults, etc.) match the README configuration table, field by field.

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
- `testAddBucketLink` - addBucketLink() returns a bucket-link ObjectInfo (link={bucket:'other-bucket'}) and serializes "link":{"bucket":"other-bucket"} (#48).
- `testUpdateMetaRenamesPreservingNuid` - updateMeta() rename preserves the NUID/size, writes new meta under the new encoded subject, tombstones the old name (deleted:true), and does NOT purge chunks (#28).
- `testUpdateMetaReplacesMetadataInPlace` - updateMeta() with no rename replaces the metadata bag in place (new map returned) writing only one meta HPUB and no tombstone (#28).
- `testListEnumeratesMetaSubjects` - list() paginates meta subjects via subjects_filter then Direct Gets each record, excluding deleted by default (1 active) and including them with includeDeleted:true (2 total).
- `testListPaginatesAcrossSubjectPages` - list() collects objects across multiple STREAM.INFO subject pages (offsets 0,1,2) returning both a.txt and b.txt rather than truncating to the first page.
- `testInfoIncludesRevisionFromSequenceHeader` - info() sets ObjectInfo.revision from the Direct Get Nats-Sequence header (revision=2).
- `testInfoFallsBackToStreamMessageWhenDirectGetUnavailable` - info() falls back to STREAM.MSG.GET when Direct Get returns 503 and still returns the metadata, exercising both API subjects.
- `testInfoReturnsNullWhenNotFound` - info() returns null when Direct Get responds 404.
- `testWatchDispatchesObjectInfo` - watch() creates a push consumer (deliver_policy:new, ack_policy:none), and on a delivered meta frame dispatches an ObjectInfo with name/nuid and revision taken from the $JS.ACK stream sequence (7).
- `testWatchWithOptionsRequestsSnapshotDeliverPolicy` - watch() given ObjectStoreWatchOptions requests deliver_policy:last_per_subject (snapshot-then-follow) instead of new (#98).
- `testWatchToleratesMalformedMetadataAndKeepsDispatching` - watch() silently skips a non-JSON meta delivery and still delivers a subsequent valid one (only the valid nuid is seen).
- `testWatchSkipsDeleteMarker` - watch() skips a server delete-marker frame (Nats-Marker-Reason) even with a valid non-empty body, while still delivering a later valid update (issue #5).
- `testInfoReturnsNullForDeleteMarker` - info() returns null when the latest meta record is a server delete-marker (Nats-Marker-Reason header) despite a non-empty body (issue #5).
- `testBucketSubjectHelpers` - streamName()/chunkPrefix()/metaPrefix() return OBJ_assets, $O.assets.C. and $O.assets.M. respectively.
- `testPutRejectsEmptyName` - put() with an empty object name throws JetStreamException "Invalid object name".
- `testPutSplitsIntoMultipleChunksWithSmallChunkSize` - put() of a 10-byte payload with chunkSize 4 splits into 3 chunks, emitting 3 chunk PUBs and recording "chunks":3.
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
- `testParsesControlLinesWithMultiSpaceAndEmbeddedTabSeparators` - asserts the #140 explode() fast path defers to the whitespace-tolerant split: multi-space-separated MSG/HMSG lines tokenize, and a tab hidden inside a space-separated token (making a 4-token-looking MSG line really the 5-token reply-to form) still yields the reply-to.
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
- `testAddEndpointRejectsEmptyName` - Asserts adding an endpoint with a blank/whitespace name throws InvalidArgumentException ("name must not be empty").
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
- `testEnqueueThrowsOnOverflowWhenPolicyIsError` - with cap 2 and Error policy, the third enqueue throws `NatsException` with message "Subscription queue overflow for sid 99".
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
- `testCompressedFrameRoundTrip` - Encodes a deflated payload as a compressed frame, asserts the RSV1 bit is set, and decode() reports rsv1=true with payload that inflates back to the original.
- `testEncodeRejectsBadMaskKey` - Asserts encode() throws ProtocolException when given a mask key that is not 4 bytes.
- `testEncode64BitLengthFrameRoundTrips` - For a 65536-byte payload asserts the 127 length marker and an 8-byte big-endian length equal to 65536, then decode() reconstructs the payload and empties the buffer.
- `testDecode64BitLengthHeaderIncompleteWaits` - A 127-marker header with only 3 of 8 length bytes yields no frames and an unchanged buffer (decode waits for the full 64-bit length).
- `testDecode16BitLengthHeaderIncompleteWaits` - A 126-marker header with only 1 of 2 length bytes yields no frames and an unchanged buffer (decode waits for the full 16-bit length).
- `testDecodePayloadLengthOutOfBoundsThrows` - A 64-bit-length header declaring MAX_FRAME_PAYLOAD+1 makes decode() throw ProtocolException matching "payload length out of bounds".
- `testDecodeMaskedFrameWaitsForMaskKey` - A masked frame header with only 2 of 4 mask-key bytes yields no frames and an unchanged buffer.
- `testInflateInvalidDataThrows` - With PHPUnit's error handler disabled, asserts inflate() on garbage input throws ProtocolException matching "inflate compressed WebSocket frame".
- `testInflateInvalidDataSuppressesNativeWarningForRespectfulHandlers` - Installs a handler that respects error_reporting() (the @ operator) and asserts inflate() on garbage still surfaces a typed ProtocolException rather than leaking an ErrorException from inflate_add().

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
- `testReadLineThrowsOnCloseFrameFromSocket` - Asserts a server CLOSE frame causes readLine() to throw TransportClosedException.

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
- `testKeyValueCreateKeyIsExclusive` - KV `createKey` succeeds first (seq>=1) but a second `createKey` on the live key throws `JetStreamException`; the value remains the first write.
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
- `testJetStreamPushConsumerHelperDelivery` - Subscribes a durable push consumer (default deliver subject), publishes one message, pumps processIncoming until received, and asserts the delivered NatsMessage payload.
- `testJetStreamPushConsumerWithExplicitDeliverSubject` - Same as the durable push helper test but with an explicit deliver subject, asserting the delivered payload.
- `testJetStreamEphemeralPushConsumerDelivery` - Subscribes an ephemeral push consumer, publishes one message, pumps until received, and asserts the delivered payload (acking when a replyTo is present).
- `testJetStreamOrderedConsumerWithFilteredSubjectAfterPriorMessages` - Advances the stream with a non-matching message first, then publishes 5 matching messages interleaved with non-matching ones; asserts the ordered consumer delivers all 5 in order and de-duplicated (P0 stream-sequence gap-detection regression guard).
- `testJetStreamOrderedConsumerReplaysPreExistingBacklogInOrder` - Publishes an 8-message backlog (interleaved with non-matching subjects) before starting the ordered consumer and asserts the full matching backlog replays in order and de-duplicated.
- `testJetStreamOrderedConsumerRecoversFromDroppedDeliveryInOrder` - Uses DroppingTransport to drop exactly the second $JS.ACK-bearing delivery, then asserts exactly one frame was dropped yet all 5 messages arrive once in order, validating the recreate + by-sequence replay and stale-delivery fence.
- `testJetStreamEphemeralPullConsumerFetchAndAck` - Creates an ephemeral consumer (asserting its streamName and non-empty name), publishes a message, fetchNext returns the payload, then acks.
- `testJetStreamKeyValueLifecycle` - Creates a KV bucket, watches "theme", puts "dark" and asserts the watch entry key/value; asserts get returns "dark"; then deletes and asserts get returns a null-value entry with operation "DEL".
- `testJetStreamKeyValueAdvancedParityOperations` - Exercises put/update (asserting seq>=2), getAll before purge (both keys present), purge (username gone, email remains), and getStatus reporting bucket, stream "KV_<bucket>", and messages>=1.
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
