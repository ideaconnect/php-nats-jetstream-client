# IDCT PHP NATS JetStream Client

[![codecov](https://codecov.io/gh/ideaconnect/php-nats-jetstream-client/graph/badge.svg?token=A816f4EXon)](https://codecov.io/gh/ideaconnect/php-nats-jetstream-client)
[![CI](https://github.com/ideaconnect/php-nats-jetstream-client/actions/workflows/ci.yml/badge.svg)](https://github.com/ideaconnect/php-nats-jetstream-client/actions/workflows/ci.yml)
[![Made in the EU](https://raw.githubusercontent.com/ideaconnect/made-in-the-eu/main/software-badge/made-in-the-eu.svg)](https://github.com/ideaconnect/made-in-the-eu)

Async-first NATS and JetStream client for PHP 8.2+ with support for core NATS messaging, JetStream, KeyValue, ObjectStore, and NATS microservices.

The library is built around Amp and provides a typed, high-level API for connection management, publish/subscribe, request/reply, reconnect handling, authentication flows, and JetStream resource management without falling back to blocking I/O.

It is intended for real application use, including service-to-service messaging, event processing, JetStream-backed persistence patterns, and NATS-based microservice discovery.

## Installation

Install from Packagist:

```bash
composer require idct/php-nats-jetstream-client
```

Package name: `idct/php-nats-jetstream-client`

Source repository: https://github.com/ideaconnect/php-nats-jetstream-client

## Index

- [Installation](#installation)
- [Features](#features)
- [NATS Server Version Requirements](#nats-server-version-requirements)
- [PHP Support Policy](#php-support-policy)
- [Usage](#usage)
- [Authentication Options](#authentication-options)
- [WebSocket Transport](#websocket-transport)
- [Connect and Publish/Subscribe](#connect-and-publishsubscribe)
- [Request/Reply](#requestreply)
- [Request Many (Scatter-Gather)](#request-many-scatter-gather)
- [Connection Statistics and RTT](#connection-statistics-and-rtt)
- [Headers and Server Info](#headers-and-server-info)
- [JetStream Stream and Durable Consumer](#jetstream-stream-and-durable-consumer)
- [JetStream Stream Update and Consumer Info](#jetstream-stream-update-and-consumer-info)
- [JetStream Pull Consumer (Fetch + ACK)](#jetstream-pull-consumer-fetch--ack)
- [JetStream Pull Consumer (NAK, Delayed NAK, TERM, In-Progress)](#jetstream-pull-consumer-nak-delayed-nak-term-in-progress)
- [Queue Group Subscribe](#queue-group-subscribe)
- [Polling Subscribe (SubscriptionQueue)](#polling-subscribe-subscriptionqueue)
- [JetStream Push Consumer (Durable)](#jetstream-push-consumer-durable)
- [JetStream Ephemeral Consumers](#jetstream-ephemeral-consumers)
- [Scheduled Publish Example (`@at`)](#scheduled-publish-example-at)
- [Distributed Counter](#distributed-counter)
- [KeyValue Bucket](#keyvalue-bucket)
- [Object Store Bucket](#object-store-bucket)
- [Object Store Streaming to Callback](#object-store-streaming-to-callback)
- [Object Store Streaming Upload](#object-store-streaming-upload)
- [Services Framework](#services-framework)
- [Services: SCHEMA Discovery](#services-schema-discovery)
- [Graceful Drain](#graceful-drain)
- [Ordered Consumer](#ordered-consumer)
- [Consumer Pause/Resume](#consumer-pauseresume)
- [Fetch Batch](#fetch-batch)
- [Stream Purge and List](#stream-purge-and-list)
- [Consumer List](#consumer-list)
- [Stream Message Get](#stream-message-get)
- [JetStream Direct Get](#jetstream-direct-get)
- [Atomic Batch Publish](#atomic-batch-publish)
- [Credentials File Authentication](#credentials-file-authentication)
- [Typed Stream Configuration](#typed-stream-configuration)
- [Pull Consumer Batching/Iteration](#pull-consumer-batchingiteration)
- [Pull Consumer Priority Groups](#pull-consumer-priority-groups)
- [Stream Mirroring and Sourcing](#stream-mirroring-and-sourcing)
- [Republish and Subject Transform](#republish-and-subject-transform)
- [Compatibility Mapping](#compatibility-mapping)
- [Behavior Notes](#behavior-notes)
  - [`processIncoming()`](#processincoming)
  - [Heartbeat and Request Timeouts](#heartbeat-and-request-timeouts)
  - [Reconnect Behavior](#reconnect-behavior)
  - [Slow Consumers](#slow-consumers)
  - [Handler Failures](#handler-failures)
  - [Ordered Consumer Gap Recovery](#ordered-consumer-gap-recovery)
- [Production Notes and Limitations](#production-notes-and-limitations)
- [Configuration Option Mapping](#configuration-option-mapping)
- [Performance Benchmark Recipe](#performance-benchmark-recipe)
- [Testing](#testing)
- [Contributing and contributors](#contributing-and-contributors)
- [Current Test Baseline](#current-test-baseline)
- [License](#license)

## Features

Current functionality includes:

- Core NATS connect/disconnect with graceful drain
- Publish and subscribe
- Request/reply with timeout and cancellation
- Reconnect with exponential backoff, server rotation, validated subscription replay, and async INFO updates
- Ping/pong heartbeat with `maxPingsOut` detection
- `max_payload` enforcement and `no_responders` negotiation
- Subject validation against NATS naming rules
- JetStream account info
- JetStream stream CRUD (create, update, get, delete, purge, list)
- JetStream consumer CRUD (durable + ephemeral, pull + push, list)
- JetStream pull consumers (fetch next, fetch batch, ACK/NAK/TERM/WPI, delayed NAK)
- JetStream push consumers with heartbeat/flow-control handling
- JetStream ordered consumers with automatic sequence tracking and gap recovery
- JetStream consumer pause/resume
- JetStream publish ACK
- JetStream stream message get by sequence - both the regular `STREAM.MSG.GET` request and the Direct Get API (`directGetStreamMessage()` / `directGetLastMessageForSubject()`)
- JetStream atomic (all-or-nothing) batch publish (`batch()` -> `BatchPublisher`, ADR-50; requires `allow_atomic`, NATS 2.12+)
- Scheduled publish (`@at` support)
- Distributed counter CRDT (atomic `incrementCounter()` / `counterValue()`, arbitrary-precision values)
- KeyValue API (bucket lifecycle with history/TTL/storage options, put/get/update/delete/purge, watch, getAll/status)
- ObjectStore API (bucket lifecycle, put/get/delete/list/watch, chunked uploads, streaming upload via `putStream()`, SHA-256 digest verification)
- Connection `flush()` (PING/PONG round-trip) to confirm the server has processed prior writes
- Request-many scatter-gather (`requestMany()`: collect multiple replies bounded by max-count / stall / timeout)
- Connection traffic statistics (`statistics()` -> `ConnectionStats`) and round-trip-time measurement (`rtt()`)
- Microservices framework (service registration, PING/INFO/STATS/SCHEMA discovery, grouped endpoints)
- Server authorization methods: token, username/password, JWT + nonce signer, built-in NKey seed signer, credentials file parser
- Standalone NKey authentication (Ed25519 challenge signing without JWT)
- `no_echo` CONNECT option
- `tlsHandshakeFirst` TLS option
- Typed JetStream configuration enums (RetentionPolicy, StorageBackend, DiscardPolicy, DeliverPolicy, AckPolicy, ReplayPolicy)
- Max frame size limit in protocol parser (DoS protection)
- Queue-based polling subscribe API (`SubscriptionQueue` with `fetch()`, `next()`, `fetchAll()`)
- Bounded per-subscription buffers with slow-consumer policies (`DropOldest`, `DropNewest`, `Error`) - see [Slow Consumers](#slow-consumers)
- Pull-consumer batching/iteration chain API (`PullConsumerIterator` with `setBatching()`, `setIterations()`, `handle()`)
- Stream mirroring and sourcing configuration helpers (`StreamSource`)
- Republish and subject transform configuration helpers (`Republish`, `SubjectTransform`)

Scheduling note: scheduled messages use the NATS scheduler headers (ADR-51) and accept `@at`, `@every`, 6-field cron, and the predefined aliases (`@daily`, `@hourly`, ...). Build expressions with `IDCT\NATS\JetStream\Schedule::at(...)`, `Schedule::atTimestamp(...)`, `Schedule::every(...)`, `Schedule::cron(...)`, or `Schedule::predefined(...)`. The target stream must be created with `allow_msg_schedules` (NATS 2.12+).

## NATS Server Version Requirements

Core NATS (publish/subscribe, request/reply, headers, services) works against any server. JetStream
consumer management requires NATS 2.9+: all consumer helpers use the named `CONSUMER.CREATE` API
introduced in 2.9, with no fallback to the legacy `DURABLE.CREATE` form. Some features depend on
even newer NATS server versions; the table below lists the minimum version per feature. Detection is
reactive - there is no per-request version probe - and depends on the feature:

- **Version-gated stream/consumer config fields** (for example creating a stream with `allow_atomic`
  or `allow_msg_schedules`): an older server rejects the unknown field and the request fails fast
  with an `IDCT\NATS\Exception\UnsupportedFeatureException` (a subclass of `JetStreamException`)
  carrying the feature name, the required version, and the version the server reported.
- **Atomic batch publish**: when the server's advertised version parses as older than 2.12,
  `commit()` fails fast before anything reaches the wire (`... nothing was published`). When the
  version cannot be parsed (a proxy, a custom build, or a mixed-version cluster), the old server
  acknowledges the batch start as a plain publish; `commit()` detects that reply shape and throws
  an `UnsupportedFeatureException` (`... treated the batch as plain publishes`) instead of silently
  storing the batch message-by-message.
- **Other data-path calls** (for example `publish(..., ttl:)` or `directGetBatch()`) against a server
  or stream without the feature surface a plain `JetStreamException` built from the server's error
  response - or the server may silently ignore an unknown header - so consult the table before
  relying on them against older servers.

| Feature | API | Min NATS | Server config / header |
| --- | --- | --- | --- |
| Named consumer create (all consumer helpers) | `createConsumer()`, `createEphemeralConsumer()`, ordered/push/pull subscribe helpers | 2.9 | n/a |
| Multi-subject consumer filters | `createConsumer(..., ['filter_subjects' => [...]])` | 2.10 | `filter_subjects` |
| Per-message / KV TTL | `publish(..., ttl:)`, `KeyValueBucket::put(..., ttl:)`, `delete/purge(..., tombstoneTtl:)` | 2.11 | `allow_msg_ttl`, `Nats-TTL` |
| Subject delete markers | KV/Object Store `watch()`/`get()` (handled automatically) | 2.11 | `subject_delete_marker_ttl`, `Nats-Marker-Reason` |
| Pull priority groups | `fetchBatch(..., $pull)`, `PullConsumerIterator::setGroup/setPriority/...`, `unpinConsumer()` | 2.11 (`prioritized` 2.12) | `priority_groups`/`priority_policy`, `Nats-Pin-Id` |
| Batched / multi Direct Get | `directGetBatch()`, `directGetLastForSubjects()` | 2.11 | `allow_direct` |
| Scheduled publishing (`@every`/cron/aliases) | `publishScheduled()`, `Schedule::every/cron/predefined` | 2.12 | `allow_msg_schedules`, `Nats-Schedule*` |
| Atomic batch publish | `batch()` -> `BatchPublisher` | 2.12 | `allow_atomic`, `Nats-Batch-*` |
| Distributed counter CRDT | `incrementCounter()`, `counterValue()` | 2.12 | `allow_msg_counter`, `Nats-Incr` |
| Publish de-duplication | `publish(..., msgId:)` | 2.2 | `Nats-Msg-Id` |

```php
use IDCT\NATS\Exception\UnsupportedFeatureException;

try {
    $js->batch()->add('orders.created', $payload)->commit()->await();
} catch (UnsupportedFeatureException $e) {
    // e.g. "Atomic batch publish requires NATS server 2.12+ (connected server 2.10.5; nothing was published)"
    echo "{$e->feature} needs NATS {$e->requiredVersion}; server is {$e->serverVersion}\n";
}
```

You can also query the requirement programmatically: `IDCT\NATS\JetStream\FeatureSupport::requiredVersion('allow_atomic')` returns `"2.12"`.

## PHP Support Policy

This library follows [PHP's official release schedule](https://www.php.net/supported-versions.php). The minimum required PHP version tracks the releases that still receive official support (active or security), and a version is dropped once it reaches end-of-life - we neither require a PHP version before it is broadly available nor keep supporting one after upstream stops patching it.

- **Current minimum: PHP 8.2.** CI runs the full test suite on every still-supported minor - currently **8.2, 8.3, 8.4, and 8.5**.
- **PHP 8.2 reaches end-of-life on 2026-12-31.** Support for PHP 8.2 will therefore be **dropped by the end of 2026**, after which the minimum becomes **PHP 8.3**. Applications that must stay on PHP 8.2 should pin to the last release made before that change.
- Mutation testing (Infection) requires PHP 8.3+ and so runs only on 8.3+ in CI, but it is a development-only tool - it does **not** affect the runtime requirement, and the library installs and runs on PHP 8.2.

## 🚀 This project looks for funding. Love my work? Support it! 💖

* ☕ **Buy me a coffee**: https://buymeacoffee.com/idct

* 💝 **Sponsor**: https://github.com/sponsors/ideaconnect

## Usage

> Every example below also ships as a runnable, self-contained script under [`examples/`](examples/) (one file per example, verified against dockerized NATS via [`scripts/run-examples.sh`](scripts/run-examples.sh)). See [examples/README.md](examples/README.md).

### Authentication Options

> 📄 **Runnable examples:** [`examples/auth-token.php`](examples/auth-token.php), [`examples/auth-userpass.php`](examples/auth-userpass.php), [`examples/auth-jwt-nkey.php`](examples/auth-jwt-nkey.php), [`examples/auth-standalone-nkey.php`](examples/auth-standalone-nkey.php), [`examples/auth-tls.php`](examples/auth-tls.php)

_Verified by: [NkeySeedSignerTest](tests/Unit/NkeySeedSignerTest.php), [NatsConnectionTest::testConnectIncludesJwtSignatureFromInfoNonce](tests/Unit/NatsConnectionTest.php); [NatsClientIntegrationTest](tests/Integration/NatsClientIntegrationTest.php) (`testTokenAuthSuccessAndFailure`, `testUserPasswordAuthSuccessAndFailure`, `testJwtNonceAuthenticationFlow`, `testStandaloneNkeyAuthenticationFlow`, `testTlsHandshakeFirstConnection`); [features/auth/](features/auth/)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Auth\NkeySeedSigner;

// Token auth.
$tokenClient = new NatsClient(new NatsOptions(
	servers: ['nats://127.0.0.1:4222'],
	token: 's3cr3t-token',
));

// Username/password.
$passwordClient = new NatsClient(new NatsOptions(
	servers: ['nats://127.0.0.1:4222'],
	username: 'alice',
	password: 's3cr3t',
));

$signer = new NkeySeedSigner('SU...USER NKEY SEED...');

$jwtClient = new NatsClient(new NatsOptions(
	servers: ['nats://127.0.0.1:4222'],
	jwt: 'your-jwt-token',
	nkey: $signer->publicKey(),
	nonceSigner: $signer,
));

// Standalone NKey (Ed25519 challenge signing, no JWT): set nkey + nonceSigner and omit jwt.
$nkeyClient = new NatsClient(new NatsOptions(
	servers: ['nats://127.0.0.1:4222'],
	nkey: $signer->publicKey(),
	nonceSigner: $signer,
));

// TLS with CA and client cert/key.
$tlsClient = new NatsClient(new NatsOptions(
	servers: ['tls://127.0.0.1:4222'],
	tlsRequired: true,
	tlsCaFile: '/path/to/ca.pem',
	tlsCertFile: '/path/to/client-cert.pem',
	tlsKeyFile: '/path/to/client-key.pem',
));
```

`NkeySeedSigner` derives the public NKey from an encoded seed and emits the base64url Ed25519 nonce signature expected by NATS servers.

`NkeySeedSigner` requires the PHP sodium extension because NATS NKey authentication uses Ed25519 challenge signing.

### WebSocket Transport

> 📄 **Runnable example:** [`examples/websocket-transport.php`](examples/websocket-transport.php)

By default `NatsClient` uses the TCP transport (`AmpSocketTransport`). To connect over WebSocket - e.g. through a NATS gateway that only exposes `ws://` / `wss://` - construct a `WebSocketTransport` and inject it. The `ws://` / `wss://` endpoint goes in `servers`; `wss://` negotiates TLS during connect (using the same `tls*` options), and the optional `webSocketHeaders` / `webSocketCompression` options apply to the upgrade handshake.

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Transport\WebSocketTransport;

$options = new NatsOptions(
    servers: ['ws://127.0.0.1:8080'],                    // or 'wss://...' for TLS
    webSocketCompression: true,                          // negotiate permessage-deflate (requires ext-zlib)
    webSocketHeaders: ['Authorization' => 'Bearer ...'], // extra headers on the upgrade request
);

$client = new NatsClient($options, new WebSocketTransport($options));
$client->connect()->await();
```

`ws://` / `wss://` URLs are only handled by `WebSocketTransport`; passing such a URL to the default TCP transport will not work. `wss://` requires `ext-openssl`, and `webSocketCompression` requires `ext-zlib`.

The transport enforces RFC 6455 and RFC 7692 strictly. A masked server-to-client frame, a fragmented or oversized control frame, `RSV1` without negotiated compression, a handshake that omits the `Upgrade` headers, and an extension response the client never offered all fail the connection instead of being tolerated. When compression is negotiated the server must echo `server_no_context_takeover`, because this codec inflates each message independently.

If you use the low-level `WebSocketFrameCodec` directly, note two contract changes in this release: `decode()` no longer throws on a strictness violation. It returns the frames it parsed before the violation and reports the violation through a new by-reference `$terminal` parameter, so no already-decoded data is discarded. It also rejects masked frames by default, since RFC 6455 forbids the server masking; pass `allowMasked: true` to decode client-written frames. `WebSocketFrameCodec::unmask()` is deprecated: use `decode(..., allowMasked: true)` instead.

_Verified by: [WebSocketTransportTest](tests/Unit/WebSocketTransportTest.php), [WebSocketFrameCodecTest](tests/Unit/WebSocketFrameCodecTest.php); live: [ClientParityIntegrationTest](tests/Integration/ClientParityIntegrationTest.php) (`testWebSocketTransportCarriesPubSubAndJetStream`, `testWebSocketCompressionAndCustomHeaders`)._

### Connect and Publish/Subscribe

> 📄 **Runnable example:** [`examples/publish-subscribe.php`](examples/publish-subscribe.php)

_Verified by: [NatsClientTest::testClientSubscribeAndProcessIncoming](tests/Unit/NatsClientTest.php); [NatsClientIntegrationTest::testFlushRoundTripConfirmsServerProcessing](tests/Integration/NatsClientIntegrationTest.php) (`flush()` round-trip); [features/core/connection.feature](features/core/connection.feature)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;

$client = new NatsClient(new NatsOptions(servers: ['nats://127.0.0.1:4222']));
$client->connect()->await();

$sid = $client->subscribe('orders.created', static function (NatsMessage $message): void {
	// Handle delivery.
	echo $message->payload . PHP_EOL;
})->await();

$client->publish('orders.created', '{"id":123}')->await();
$client->processIncoming()->await();

$client->unsubscribe($sid)->await();
$client->disconnect()->await();
```

### Request/Reply

> 📄 **Runnable example:** [`examples/request-reply.php`](examples/request-reply.php)

_Verified by: [NatsClientTest::testClientRequestReturnsReply](tests/Unit/NatsClientTest.php), [NatsConnectionTest::testRequestReturnsFirstReplyMessage](tests/Unit/NatsConnectionTest.php); [features/core/request_reply.feature](features/core/request_reply.feature)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$reply = $client->request('svc.echo', '{"hello":"world"}', 2000)->await();
echo $reply->payload . PHP_EOL;

$client->disconnect()->await();
```

`request()`, `requestWithHeaders()` and `requestMany()` share one reply-inbox subscription per connection, `_INBOX.<inbox>.*`, which the first request subscribes. The write that subscribes it carries a PING, and the PONG tells the client that the server took the subscription. When the server rejects it, request/reply fails fast instead of every request waiting out its timeout:

- for permissions (the account may not subscribe to `_INBOX.>`), every request fails with a `ConnectionException` that names the permission needed, until the connection closes for good and a new `connect()` tries again. A request already waiting on the inbox fails as soon as the `-ERR` is read, whichever fiber's read meets it - its own, an application `processIncoming()` loop's, the heartbeat's - its wait ended, on the socket or for the read slot another operation's read holds (#180); an error listener that answers the violation with `disconnect()` ends such a request with `Connection is not open` instead;
- for the connection's subscription limit (`maximum subscriptions exceeded`, an `-ERR` that names no subject), the requests waiting on the inbox fail as soon as the `-ERR` is read, whichever fiber's read meets it, their waits ended, on the socket or for the read slot (#180): a request already sent whose own read meets the `-ERR` fails with the server's error, any other with `request/reply failed: the server may have rejected the shared reply-inbox subscription ...`, and a `requestMany()` that has collected replies returns them. The next request subscribes the inbox again and waits until the server has taken the new subscription before it is sent, so while the limit holds requests fail without reaching the responder, and once a slot is free they work again, with no reconnect. Each of those requests costs a round trip (an UNSUB, a SUB and a PING out, an `-ERR` and a PONG back) and nothing throttles them, so a caller that retries should pause between attempts. Requests issued before the server has answered the first subscription after a connect, or while a reconnect's replay of the inbox is unconfirmed, are sent at once, as before, so they reach the responder even when the inbox is rejected.

A reconnect whose replay of the inbox is rejected is handled the same way, and an inbox the server has taken stays in place when another subscription is rejected. Another subscription's `-ERR` that arrives while the inbox's SUB is still on its way, or before its PONG, drops an inbox the server takes after all; the next request's write unsubscribes it, so the slot is not lost. A `drain()` keeps the inbox subscribed while it delivers, for the requests your handlers make then, and unsubscribes it once its delivery phase is over (#213, see [Graceful Drain](#graceful-drain)): a request whose set-up the drain overtakes, while the inbox's SUB is on its way or while the request waits for the server to take it, is sent once its set-up is over and gets its reply, and one whose set-up is still under way when that phase ends fails with `Connection is not open`, without being sent.

The request that subscribes the inbox does so within its own budget - its timeout and its caller's cancellation - like the rest of the request: a SUB held up by backpressure on the socket, or the reconnect its failed write starts, no longer keeps it waiting past either (#194, #213). Requests that join its set-up wait within their own budgets too, and when the request that writes the SUB stops waiting for it, its SUB, if already out, is followed by an UNSUB, and a request that joined it subscribes the inbox itself rather than fail with the other request's timeout; a rejection or a closed connection still fails them all. _Verified by: [MuxInboxRejectionTest](tests/Unit/MuxInboxRejectionTest.php), [MuxInboxSetUpBudgetTest](tests/Unit/MuxInboxSetUpBudgetTest.php), [MuxInboxDropWakeupTest](tests/Unit/MuxInboxDropWakeupTest.php), [NatsConnectionTest::testRequestSurfacesMuxInboxPermissionRejection](tests/Unit/NatsConnectionTest.php)._

### Request Many (Scatter-Gather)

> 📄 **Runnable example:** [`examples/request-many.php`](examples/request-many.php)

_Verified by: [NatsConnectionTest](tests/Unit/NatsConnectionTest.php) (`testRequestManyCollectsUpToMaxResponses`, `testRequestManyStopsOnStallInterval`, `testRequestManyReturnsEmptyOnNoResponders`)._

`requestMany()` sends a single request and collects MULTIPLE replies (scatter-gather), returning a `list<NatsMessage>`. Collection stops on the first of: `$maxResponses` replies, a `no_responders` sentinel (returns `[]`), the per-message `$stallMs` gap, or `$totalTimeoutMs`.

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

// Stop after 3 replies, or after the 2000ms total budget, whichever comes first.
$replies = $client->requestMany('svc.scan', 'who-is-there', null, 3, 2000)->await();

foreach ($replies as $reply) {
	echo $reply->payload . PHP_EOL;
}

// Time-bounded only: keep collecting until 200ms pass with no new reply.
$discovered = $client->requestMany('svc.scan', 'ping', null, null, 5000, 200)->await();
echo count($discovered) . ' responders' . PHP_EOL;

$client->disconnect()->await();
```

### Connection Statistics and RTT

> 📄 **Runnable example:** [`examples/connection-stats-rtt.php`](examples/connection-stats-rtt.php)

_Verified by: [NatsConnectionTest](tests/Unit/NatsConnectionTest.php) (`testConnectionAccessorsAndStatistics`, `testRttMeasuresPingPong`)._

`statistics()` returns an immutable `ConnectionStats` snapshot of traffic counters (`inMsgs`, `outMsgs`, `inBytes`, `outBytes`, `reconnects`). `rtt()` measures the round-trip time to the server with a PING/PONG exchange and resolves to a `float` in seconds.

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$client->publish('events.orders', '{"id":1}')->await();

$stats = $client->statistics();
echo "out: {$stats->outMsgs} msgs / {$stats->outBytes} bytes" . PHP_EOL;
echo "in:  {$stats->inMsgs} msgs / {$stats->inBytes} bytes" . PHP_EOL;
echo "reconnects: {$stats->reconnects}" . PHP_EOL;

$rttSeconds = $client->rtt()->await();
echo 'rtt: ' . round($rttSeconds * 1000, 2) . ' ms' . PHP_EOL;

$client->disconnect()->await();
```

### Headers and Server Info

> 📄 **Runnable example:** [`examples/headers-and-server-info.php`](examples/headers-and-server-info.php)

_Verified by: [NatsClientTest::testClientPublishWithHeadersAndRequestWithHeaders](tests/Unit/NatsClientTest.php), [NatsClientTest::testClientConnectAndPublishDelegatesToConnection](tests/Unit/NatsClientTest.php), [NatsHeadersTest::testAHeaderNameThatIsADecimalIntegerComesBackAsAnIntKey](tests/Unit/NatsHeadersTest.php); [features/core/headers_queueing.feature](features/core/headers_queueing.feature)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsHeaders;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$client->publishWithHeaders('events.orders', '{"id":123}', [
	'Nats-Msg-Id' => 'orders-123',
	'Content-Type' => 'application/json',
])->await();

$reply = $client->requestWithHeaders('svc.echo', 'hello', [
	'X-Request-Id' => 'req-123',
], 2000)->await();

// Read headers off a delivered message. get() matches the name case-insensitively, which
// matters because publishers differ in how they canonicalize names (nats.go canonicalizes
// on read, so a Go publisher's "Nats-Msg-Id" may arrive as "nats-msg-id").
$headers = NatsHeaders::fromWireBlock($reply->rawHeaders);
echo NatsHeaders::get($headers, 'x-request-id') ?? '(none)', PHP_EOL;

echo $reply->payload . PHP_EOL;
echo $client->serverInfo()?->serverName . PHP_EOL;

$client->disconnect()->await();
```

Header names must be non-empty tokens without whitespace or `:`, and values must not contain CR
or LF; both throw `InvalidArgumentException`. A value may also be a `list<string>` to emit one
line per element (ADR-4 multi-value headers).

Values are written to the wire **verbatim**: surrounding whitespace is preserved byte for byte
(nats.go parity), so a signature or checksum carried in a header is not mutated on the way out.
The decoder still trims surrounding whitespace when reading, as an inbound tolerance. Use
`NatsHeaders::fromWireBlock()` to decode a delivered `rawHeaders` block (`fromWireBlockMulti()`
keeps repeated names) and `NatsHeaders::get()` for case-insensitive lookups; an exact-case match
always wins over a case-insensitive one. A header name that is a decimal integer, such as `1`, comes
back as an int key, as PHP stores such array keys, so cast a name to string before passing it to a
string function: whoever publishes a message chooses its headers. Both decoders declare their keys
as `int|string`, so static analysis points out such a call.

### JetStream Stream and Durable Consumer

> 📄 **Runnable example:** [`examples/jetstream-stream-and-consumer.php`](examples/jetstream-stream-and-consumer.php)

_Verified by: [JetStreamContextTest](tests/Unit/JetStreamContextTest.php) (`testStreamCrud`, `testConsumerCrud`, `testPublishWithAck`, `testCreateConsumerDefaultsAckPolicyToExplicit`); [JetStreamIntegrationTest::testJetStreamConsumerAndPublishAck](tests/Integration/JetStreamIntegrationTest.php); [features/jetstream-core/stream_lifecycle.feature](features/jetstream-core/stream_lifecycle.feature)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$js = $client->jetStream();
$js->createStream('ORDERS', ['orders.>'])->await();
// If you omit ack_policy, helper methods default it to explicit.
// Pass ack_policy explicitly when you need none/all.
$js->createConsumer('ORDERS', 'PROC', 'orders.created')->await();

$ack = $js->publish('orders.created', '{"id":123}')->await();
echo $ack->stream . ':' . $ack->seq . PHP_EOL;

$js->deleteConsumer('ORDERS', 'PROC')->await();
$js->deleteStream('ORDERS')->await();
$client->disconnect()->await();
```

### JetStream Stream Update and Consumer Info

> 📄 **Runnable example:** [`examples/stream-update-and-consumer-info.php`](examples/stream-update-and-consumer-info.php)

_Verified by: [JetStreamContextTest](tests/Unit/JetStreamContextTest.php) (`testUpdateStream`, `testConsumerCrud`); [JetStreamIntegrationTest::testJetStreamUpdateStreamConfiguration](tests/Integration/JetStreamIntegrationTest.php); [features/jetstream-core/management.feature](features/jetstream-core/management.feature)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$js = $client->jetStream();
$js->createStream('ORDERS', ['orders.created'])->await();
$js->updateStream('ORDERS', [
	'subjects' => ['orders.created', 'orders.updated'],
])->await();

$js->createConsumer('ORDERS', 'PROC', 'orders.created')->await();
$consumerInfo = $js->getConsumer('ORDERS', 'PROC')->await();

echo $consumerInfo->streamName . PHP_EOL;
echo $consumerInfo->name . PHP_EOL;

$js->deleteConsumer('ORDERS', 'PROC')->await();
$js->deleteStream('ORDERS')->await();
$client->disconnect()->await();
```

Idempotent upserts are available when you do not want to branch on "exists vs. not": `createOrUpdateStream()` creates the stream or falls back to updating it when the name is already in use, and `addOrUpdateConsumer()` creates or updates a durable consumer. Both mirror nats.go / nats.java `CreateOrUpdateStream` / `CreateOrUpdateConsumer`.

_Verified by: [JetStreamContextTest](tests/Unit/JetStreamContextTest.php) (`testCreateOrUpdateStreamFallsBackToUpdate`, `testAddOrUpdateConsumerDelegatesToCreateConsumer`)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$js = $client->jetStream();

// Create the stream, or update its config if it already exists.
$js->createOrUpdateStream('ORDERS', ['orders.created', 'orders.updated'])->await();

// Create or update a durable consumer in one idempotent call.
$consumer = $js->addOrUpdateConsumer('ORDERS', 'PROC', 'orders.created')->await();
echo $consumer->name . PHP_EOL;

$client->disconnect()->await();
```

### JetStream Pull Consumer (Fetch + ACK)

> 📄 **Runnable example:** [`examples/pull-consumer-fetch-ack.php`](examples/pull-consumer-fetch-ack.php)

_Verified by: [JetStreamContextTest](tests/Unit/JetStreamContextTest.php) (`testFetchNext`, `testFetchBatchThrowsTerminalStatusDescription`); [JetStreamIntegrationTest::testJetStreamPullFetchAndAck](tests/Integration/JetStreamIntegrationTest.php); [features/jetstream-core/consumer_helpers.feature](features/jetstream-core/consumer_helpers.feature)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$js = $client->jetStream();
$js->createStream('ORDERS', ['orders.created'])->await();
$js->createConsumer('ORDERS', 'PULL', 'orders.created')->await();
$js->publish('orders.created', '{"id":123}')->await();

$message = $js->fetchNext('ORDERS', 'PULL', 3000)->await();
$js->ack($message)->await();

$client->disconnect()->await();
```

When a pull request ends with a terminal JetStream status frame and no user message is delivered, `fetchNext()` / `fetchBatch()` raise `JetStreamException` with the server status code and description, for example `JetStream pull request ended with status 404: No Messages`.

At the connection's subscription limit the server rejects the fetch's reply inbox with `-ERR 'maximum subscriptions exceeded'`, which names no subject, and keeps the connection open. The client writes a `PING` behind the inbox's `SUB`, in the same write: the `PONG` answering it, or a delivery on the inbox, confirms that the server holds the inbox. A limit `-ERR` read before that fails the fetch at once, whichever read meets it: the fetch's own read fails it with the server's error (`Server sent error frame: 'maximum subscriptions exceeded'`); the heartbeat's read, or an application's `processIncoming()` loop's, fails it with a `ConnectionException` naming the inbox and the limit (`JetStream pull fetch failed: the server may have rejected its reply-inbox subscription "_INBOX.JS.FETCH.<nuid>" because the connection is at its subscription limit (maximum subscriptions exceeded). A retry subscribes a new one.`). Either way the inbox is unsubscribed, the connection stays open, and a retry once a subscription slot is free works. Such an `-ERR` cannot be told from another unconfirmed inbox's, so several inboxes unconfirmed at once - two fetches, a fetch and the shared reply inbox of `request()` - are all treated as rejected, and each call fails. A reconnect replays the inbox with a `PING` behind it again, under the same rule: a fetch whose replayed inbox the new server rejects fails with the limit error, its `SUB` write failed and replayed or not, and the replay unsubscribes the inbox. A permissions violation naming the inbox fails the fetch at once as well, quoting it. Before this rule a fetch learnt of the rejection only through its own read: when another read took the `-ERR`, it waited out its expiry and reported an empty pull (a 408) (#175).

_Verified by: [JetStreamInboxRejectionTest](tests/Unit/JetStreamInboxRejectionTest.php) (`testAFetchWhoseInboxSubIsRejectedWhileTheHeartbeatReadsTheErrFailsAtOnce`, `testAFetchWhoseInboxSubIsRejectedWhileAnApplicationLoopReadsTheErrFailsAtOnce`, `testTheOperationsOwnReadMeetingTheErrFailsItWithTheServersErrorAsBefore`, `testThePongBehindTheSubConfirmsTheInboxWhichALaterLimitErrLeavesAlone`, `testADeliveryOnTheInboxConfirmsItBeforeThePongBehindItsSub`, `testTwoInboxesUnconfirmedAtOnceAreBothTreatedAsRejected`, `testAnInboxUnconfirmedTogetherWithTheReplyInboxIsTreatedAsRejectedWithIt`, `testAPermissionsViolationNamingTheInboxFailsTheOperationAtOnce`, `testAFetchWhoseReplayedInboxIsRejectedByTheNewServerFailsWithTheLimitError`, `testAFetchWhoseSubWriteFailedAndWhoseReplayIsRejectedFailsWithTheLimitError`)._

For exactly-once-style processing use `ackSync()` instead of `ack()`: it sends the `+ACK` as a request and waits for the server to confirm the acknowledgement was durably recorded (double-ack). It throws if the delivered message carries no reply subject, and throws `TimeoutException` if no confirmation arrives within the optional timeout.

_Verified by: [JetStreamContextTest](tests/Unit/JetStreamContextTest.php) (`testAckSyncSendsAckAsRequestAndAwaitsConfirmation`, `testAckSyncThrowsForEmptyReplySubject`)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$js = $client->jetStream();
$message = $js->fetchNext('ORDERS', 'PULL', 3000)->await();

// Double-ack: block until the server confirms the ACK (250ms confirmation timeout).
$js->ackSync($message, 250)->await();

$client->disconnect()->await();
```

### JetStream Pull Consumer (NAK, Delayed NAK, TERM, In-Progress)

> 📄 **Runnable example:** [`examples/pull-consumer-nak-term.php`](examples/pull-consumer-nak-term.php)

_Verified by: [JetStreamContextTest::testAckHelpersPublishProtocolTokens](tests/Unit/JetStreamContextTest.php); [JetStreamIntegrationTest](tests/Integration/JetStreamIntegrationTest.php) (`testJetStreamPullNakWithDelayRedelivery`, `testJetStreamTermAndInProgressTokens`); [features/jetstream-core/consumer_helpers.feature](features/jetstream-core/consumer_helpers.feature)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$js = $client->jetStream();
$js->createStream('JOBS', ['jobs.>'])->await();
$js->createConsumer('JOBS', 'WORKER', 'jobs.>')->await();
$js->publish('jobs.process', '{"task":"rebuild"}')->await();

$message = $js->fetchNext('JOBS', 'WORKER', 3000)->await();

// Signal work-in-progress to extend the ack deadline.
$js->inProgress($message)->await();

// NAK: redeliver the message immediately.
$js->nak($message)->await();

// NAK with delay: redeliver after 5 seconds.
// $js->nakWithDelay($message, 5000)->await();

// TERM: terminate delivery, do not redeliver.
// $js->term($message)->await();

$js->deleteConsumer('JOBS', 'WORKER')->await();
$js->deleteStream('JOBS')->await();
$client->disconnect()->await();
```

### Queue Group Subscribe

> 📄 **Runnable example:** [`examples/queue-group-subscribe.php`](examples/queue-group-subscribe.php)

_Verified by: [NatsConnectionTest::testSubscribeWithQueueGroupSendsSubFrameAndDeliversToHandler](tests/Unit/NatsConnectionTest.php); [features/core/headers_queueing.feature](features/core/headers_queueing.feature)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

// Subscribe with a queue group for load-balanced delivery across workers.
$sid = $client->subscribe('tasks.process', static function (NatsMessage $message): void {
	echo 'Worker received: ' . $message->payload . PHP_EOL;
}, queue: 'workers')->await();

$client->publish('tasks.process', '{"job":"build"}')->await();
$client->processIncoming()->await();

$client->unsubscribe($sid)->await();
$client->disconnect()->await();
```

### Polling Subscribe (SubscriptionQueue)

> 📄 **Runnable example:** [`examples/polling-subscribe.php`](examples/polling-subscribe.php)

_Verified by: [SubscriptionQueueTest](tests/Unit/SubscriptionQueueTest.php) (`fetch`/`next`/`fetchAll`/`setTimeout`); [ConcurrentReadTest::testPollReturnsAMessageAnotherReadDeliversDuringItsPause](tests/Unit/ConcurrentReadTest.php); [OperationReadWakeupTest](tests/Unit/OperationReadWakeupTest.php); [ReadWakeupTest](tests/Unit/ReadWakeupTest.php); [ReadWakeupLifecycleTest](tests/Unit/ReadWakeupLifecycleTest.php); [NatsClientIntegrationTest::testSubscriptionQueuePollingDeliversLive](tests/Integration/NatsClientIntegrationTest.php); [features/core/headers_queueing.feature](features/core/headers_queueing.feature)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

// subscribeQueue() returns a SubscriptionQueue for polling-style consumption.
$queue = $client->subscribeQueue('events.>', queue: 'workers')->await();
$queue->setTimeout(5.0);

// Non-blocking fetch - returns null if nothing available.
$msg = $queue->fetch();

// Blocking fetch - waits up to the configured timeout, returns null on timeout.
// With no timeout configured it performs a single processIncoming() cycle (like fetch()).
$msg = $queue->next();

// Batch fetch - collects up to 10 messages within the timeout window.
$messages = $queue->fetchAll(limit: 10);

$client->unsubscribe($queue->sid)->await();
$client->disconnect()->await();
```

A poll with a timeout returns a message as soon as it is in the queue, whichever read put it there: its own, or another fiber's, such as your own `processIncoming()` loop's. That holds when the delivery reaches the queue between the poll's reads and when it reaches it while the poll's read already waits, on the socket, behind that other read or for a reconnect: the read ends and the poll looks again. `next()` returns the message, and `fetchAll()` returns once it holds `limit` messages. The poll also takes what is already queued for its subscription before it reads: a message already in the queue's buffer, or one queued behind a delivery that another fiber's read is still running - a handler of another subscription ahead of it that awaits something for longer than the poll's timeout - the poll takes at once instead of reading (#179). This is what lets a subscription handler poll a queue of its own for a message delivered in the same chunk as the handler's: that message is queued behind the handler's own delivery, and the poll takes it from inside the handler. Only the poll's own subscription is taken ahead; the other subscriptions' queued messages keep their order for the delivery under way. The take is of what is already there when the poll looks, so two cases are unchanged: a poll whose own read brings the chunk runs any handler ahead of its message in that read's own delivery pass and returns the message once the handler returns, however long it takes; and while a reconnect is under way the poll waits for the reconnect first, within its timeout, and takes a message already queued for it once the connection is back.

A queue's buffer is bounded like its subscription's. If your application does not poll often enough, messages are dropped under the slow-consumer policy - see [Slow Consumers](#slow-consumers) for which ones, and how the loss is reported.

### JetStream Push Consumer (Durable)

> 📄 **Runnable example:** [`examples/push-consumer-durable.php`](examples/push-consumer-durable.php)

_Verified by: [JetStreamContextTest](tests/Unit/JetStreamContextTest.php) (`testSubscribePushConsumerHandlesFlowControl`, `testSubscribePushConsumerIgnoresHeartbeat`); [JetStreamIntegrationTest](tests/Integration/JetStreamIntegrationTest.php) (`testJetStreamPushConsumerHelperDelivery`, `testJetStreamPushFlowControlAndHeartbeat`); [features/jetstream-core/consumer_helpers.feature](features/jetstream-core/consumer_helpers.feature)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$js = $client->jetStream();
$js->createStream('ORDERS', ['orders.created'])->await();

$sid = $js->subscribePushConsumer(
	stream: 'ORDERS',
	consumer: 'PUSH_PROC',
	handler: static function (NatsMessage $message) use ($js): void {
		// Heartbeats / flow-control are handled automatically by helper.
		$js->ack($message)->await();
	},
	filterSubject: 'orders.created',
)->await();

$js->publish('orders.created', '{"id":123}')->await();
$client->processIncoming()->await();

$client->unsubscribe($sid)->await();
$js->deleteConsumer('ORDERS', 'PUSH_PROC')->await();
$js->deleteStream('ORDERS')->await();
$client->disconnect()->await();
```

### JetStream Ephemeral Consumers

> 📄 **Runnable example:** [`examples/ephemeral-consumers.php`](examples/ephemeral-consumers.php)

_Verified by: [JetStreamContextTest](tests/Unit/JetStreamContextTest.php) (`testCreateEphemeralConsumer`, `testSubscribeEphemeralPushConsumer`); [JetStreamIntegrationTest](tests/Integration/JetStreamIntegrationTest.php) (`testJetStreamEphemeralPullConsumerFetchAndAck`, `testJetStreamEphemeralPushConsumerDelivery`); [features/jetstream-core/consumer_helpers.feature](features/jetstream-core/consumer_helpers.feature)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$js = $client->jetStream();
$js->createStream('ORDERS', ['orders.created'])->await();

// Ephemeral pull consumer.
$ephemeral = $js->createEphemeralConsumer('ORDERS', 'orders.created')->await();
$js->publish('orders.created', '{"id":123}')->await();
$pullMessage = $js->fetchNext('ORDERS', $ephemeral->name)->await();
$js->ack($pullMessage)->await();

// Ephemeral push consumer.
$js->subscribeEphemeralPushConsumer(
	stream: 'ORDERS',
	handler: static function (NatsMessage $message) use ($js): void {
		$js->ack($message)->await();
	},
	filterSubject: 'orders.created',
)->await();

$client->disconnect()->await();
```

### Scheduled Publish Example (`@at`)

> 📄 **Runnable example:** [`examples/scheduled-publish.php`](examples/scheduled-publish.php)

_Verified by: [ScheduleTest](tests/Unit/ScheduleTest.php), [JetStreamContextTest](tests/Unit/JetStreamContextTest.php) (`testPublishScheduled`, `testPublishScheduledOmitsTtlWhenNotProvided`, `testPublishScheduledRejectsUnsupportedPattern`); [JetStreamIntegrationTest](tests/Integration/JetStreamIntegrationTest.php) (`testJetStreamScheduledPublish`, `testJetStreamScheduledPublishWithPerMessageTtl`, `testJetStreamScheduledPublishRejectsUnsupportedPatterns`); [features/jetstream-data/scheduled_publish.feature](features/jetstream-data/scheduled_publish.feature)._

Prerequisites: the backing stream must be created with `allow_msg_schedules: true`, and because this example sets `scheduleTtl`, also `allow_msg_ttl: true`. The stream's subject list must cover both the schedule subject and the target subject. Without these flags the server rejects the publish with `message schedules is disabled` or `per-message TTL is disabled`.

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\JetStream\Schedule;
use DateTimeImmutable;

$client = new NatsClient(new NatsOptions(servers: ['nats://127.0.0.1:4222']));
$client->connect()->await();

$jetStream = $client->jetStream();

// The backing stream must cover the schedule and target subjects and enable scheduling.
// allow_msg_schedules is required for scheduled publish; allow_msg_ttl is required when
// you pass scheduleTtl.
$jetStream->createStream('ORDERS', [
	'schedules.orders.one',
	'events.orders',
], [
	'allow_msg_schedules' => true,
	'allow_msg_ttl' => true,
])->await();

$jetStream->publishScheduled(
	scheduleSubject: 'schedules.orders.one',
	targetSubject: 'events.orders',
	payload: json_encode(['id' => 123], JSON_THROW_ON_ERROR),
	schedule: Schedule::at(new DateTimeImmutable('+30 seconds')),
	scheduleTtl: '5m',
)->await();

$client->disconnect()->await();
```

### Distributed Counter

> 📄 **Runnable example:** [`examples/distributed-counter.php`](examples/distributed-counter.php)

_Verified by: [JetStreamContextTest](tests/Unit/JetStreamContextTest.php) (`testIncrementCounter`, `testIncrementCounterPreservesBigValue`, `testIncrementCounterRejectsMalformedDelta`, `testCounterValue`, `testCounterValueMissingReturnsZero`, `testCounterValueRethrowsNon404Exception`, `testIncrementCounterWithMalformedResponsePayload`, `testIncrementCounterWithApiErrorInResponse`, `testIncrementCounterWithIntegerValField`, `testIncrementCounterWithMissingValFieldThrows`)._

Distributed counters are an atomic, conflict-free (CRDT) increment subject backed by a JetStream stream. `incrementCounter()` applies a signed delta and returns the new total; `counterValue()` reads the current total via Direct Get (returning `"0"` when nothing is stored yet).

Requires NATS server 2.12+, and the backing stream must be created with `allow_msg_counter: true` (and `allow_direct: true`, since `counterValue()` reads via Direct Get). On a pre-2.12 server, `createStream()` with the counter flag fails fast with an `UnsupportedFeatureException` (the server rejects the unknown config field). A stream created without the flag causes the increment request to be rejected, surfaced as a `JetStreamException`.

The delta is passed as an integer **string** (e.g. `"+5"`, `"-3"`, `"10"`); a malformed delta is rejected before dispatch. Counter totals are likewise returned as **strings** so values beyond `PHP_INT_MAX` are preserved exactly rather than truncated to a float.

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$client = new NatsClient(new NatsOptions(servers: ['nats://127.0.0.1:4222']));
$client->connect()->await();

$js = $client->jetStream();

// The backing stream must enable allow_msg_counter (NATS 2.12+). allow_direct is required because
// counterValue() reads the current total via Direct Get.
$js->createStream('COUNTERS', ['counters.>'], [
	'allow_msg_counter' => true,
	'allow_direct' => true,
])->await();

// Atomically increment; the new total is returned as a string.
$total = $js->incrementCounter('counters.visits', '+5')->await();
echo "After increment: {$total}\n"; // "5"

$js->incrementCounter('counters.visits', '+3')->await();
$js->incrementCounter('counters.visits', '-1')->await();

// Read the current value via Direct Get ("0" if nothing stored yet).
$current = $js->counterValue('COUNTERS', 'counters.visits')->await();
echo "Current value: {$current}\n"; // "7"

$client->disconnect()->await();
```

### KeyValue Bucket

> 📄 **Runnable example:** [`examples/keyvalue-bucket.php`](examples/keyvalue-bucket.php)

_Verified by: [KeyValueBucketTest](tests/Unit/KeyValueBucketTest.php); [JetStreamIntegrationTest](tests/Integration/JetStreamIntegrationTest.php) (`testJetStreamKeyValueLifecycle`, `testJetStreamKeyValueAdvancedParityOperations`); [features/jetstream-data/key_value.feature](features/jetstream-data/key_value.feature)._

```php
<?php

declare(strict_types=1);

use Amp\CancelledException;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\JetStream\KeyValue\KeyValueEntry;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$js = $client->jetStream();
$kv = $js->keyValue('cfg');
$kv->create()->await();

// Register the watcher BEFORE the writes it should observe: watch() delivers live updates only
// (deliver_policy=new) and does not replay pre-existing values. Each entry carries its revision.
$watchSid = $kv->watch(static function (KeyValueEntry $entry): void {
	echo $entry->key . ':' . ($entry->value ?? '<deleted>') . ' (rev ' . ($entry->revision ?? 0) . ')' . PHP_EOL;
}, 'theme')->await();

$kv->put('theme', 'dark')->await();
$entry = $kv->get('theme')->await();
echo $entry?->value . PHP_EOL;

if ($entry !== null) {
	$kv->update('theme', 'light', $entry->revision ?? 1)->await();
}

$all = $kv->getAll()->await();
echo ($all['theme'] ?? '') . PHP_EOL;

$status = $kv->getStatus()->await();
echo $status['stream'] . PHP_EOL;

$kv->delete('theme')->await();
$kv->purge('theme')->await();

// Drive delivery so the watcher receives the buffered updates, bounded so it cannot block forever.
try {
	$cancellation = new TimeoutCancellation(2.0);
	while (true) {
		$client->processIncoming($cancellation)->await();
	}
} catch (CancelledException) {
}

$js->stopOrderedConsumer($watchSid)->await(); // watches ride ordered consumers: recreates rotate the internal sid, so stop via the context, not a plain unsubscribe
$kv->deleteBucket()->await();
$client->disconnect()->await();
```

#### Mirrored and sourced KV buckets

A bucket can be created with `sources` (copy entries in from other buckets, re-subjected into this
bucket's own prefix per ADR-57) or with `mirror` (a read replica that writes through to the origin).
Bucket names are translated to their backing `KV_`-prefixed streams for you. The `bucket` alias is
always prefixed, so `['bucket' => 'KV_x']` means the bucket literally named `KV_x`; an explicit
`name` (or a bare string entry) follows the nats.go convention of being used as-is when it already
starts with `KV_`. Supply your own `subject_transforms` to source a non-KV stream verbatim.

Mirror routing lives on the handle: the instance that ran `create()` resolves it automatically, but
any other handle for the same bucket, including a fresh `keyValue()` in the same process, must call
`bind()` first. `bind()` reads `STREAM.INFO` and resolves the read and write prefixes, so reads hit
the origin's subjects and writes go through to the origin instead of into a stream that ingests
nothing:

```php
$mirror = $js->keyValue('cfg-replica');
$mirror->bind()->await();   // required on a handle that did not create the bucket

$mirror->put('theme', 'dark')->await();   // written through to the origin bucket
echo $mirror->get('theme')->await()?->value, PHP_EOL;
```

To read a specific historical revision (stream sequence) of a key, use `getRevision()`. It returns `null` when nothing is stored at that sequence or when the sequence belongs to a different key, and throws for a non-positive revision.

_Verified by: [KeyValueBucketTest](tests/Unit/KeyValueBucketTest.php) (`testGetRevisionReturnsEntryAtSequence`, `testGetRevisionReturnsNullForDifferentKey`)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$kv = $client->jetStream()->keyValue('cfg');

// Load the value of 'theme' as it existed at stream sequence 2.
$historical = $kv->getRevision('theme', 2)->await();
echo $historical?->value ?? '<none>', PHP_EOL;

$client->disconnect()->await();
```

### Object Store Bucket

> 📄 **Runnable example:** [`examples/object-store-bucket.php`](examples/object-store-bucket.php)

_Verified by: [ObjectStoreBucketTest](tests/Unit/ObjectStoreBucketTest.php); [JetStreamIntegrationTest::testJetStreamObjectStoreLifecycle](tests/Integration/JetStreamIntegrationTest.php); [features/jetstream-data/object_store.feature](features/jetstream-data/object_store.feature)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$store = $client->jetStream()->objectStore('assets');
$store->create()->await();

$stored = $store->put('logo.txt', 'hello-object', ['content-type' => 'text/plain'])->await();
echo $stored->name . PHP_EOL;

$info = $store->info('logo.txt')->await();
echo $info?->digest . PHP_EOL;

$objectData = $store->get('logo.txt')->await();
echo $objectData?->data . PHP_EOL;

$objects = $store->list()->await();
foreach ($objects as $object) {
	echo $object->name . PHP_EOL;
}

$store->delete('logo.txt')->await();
$store->deleteBucket()->await();
$client->disconnect()->await();
```

Buckets and objects support extra management operations. `seal()` makes a bucket permanently read-only (irreversible). `addLink()` / `addBucketLink()` create link objects pointing at another object or a whole bucket. `updateMeta()` renames an object and/or replaces its metadata WITHOUT re-uploading its bytes (the stored chunks are kept by NUID).

`addLink()` refuses a name that any non-link record already holds, including the tombstone of a
deleted object (nats.go `ErrObjectAlreadyExists` parity). Re-pointing an existing link is allowed;
freeing a name that once held a real object is not, so pick a fresh name for the link.

_Verified by: [ObjectStoreBucketTest](tests/Unit/ObjectStoreBucketTest.php) (`testSeal`, `testAddLink`, `testAddBucketLink`, `testUpdateMetaRenamesPreservingNuid`, `testUpdateMetaReplacesMetadataInPlace`)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$store = $client->jetStream()->objectStore('assets');
$store->create()->await();
$store->put('real.bin', 'payload')->await();
$store->put('logo.txt', 'logo')->await();

// Link to another object (optionally in a different bucket).
$store->addLink('shortcut', 'real.bin')->await();

// Link to a whole bucket.
$store->addBucketLink('mirror', 'other-bucket')->await();

// Rename without re-uploading; the stored chunks are preserved by NUID.
$store->updateMeta('logo.txt', 'brand.txt')->await();

// Replace only the metadata bag (no rename, no re-upload).
$store->updateMeta('brand.txt', null, ['team' => 'brand'])->await();

// Make the bucket permanently read-only (irreversible).
$store->seal()->await();

$client->disconnect()->await();
```

#### Watching an Object Store bucket

> 📄 **Runnable example:** [`examples/object-store-watch.php`](examples/object-store-watch.php)

`watch()` reports object metadata changes (puts, deletes, link creation) as they happen. Without
options it delivers only updates published after the watch starts; pass an `ObjectStoreWatchOptions`
to replay the current metadata of every existing object first, take full history, or go
updates-only. Like the KV watch it rides an ordered consumer, so a slow consumer or a reconnect
window is replayed rather than lost, and it requests an idle heartbeat by default (tune it with
`ObjectStoreWatchOptions::$idleHeartbeat`). Stop it with `stopOrderedConsumer()`:

```php
$store = $js->objectStore('assets');

$sid = $store->watch(static function (ObjectInfo $info): void {
	echo $info->name . ' changed' . PHP_EOL;
}, options: new ObjectStoreWatchOptions(idleHeartbeat: 10_000_000_000))->await();

// ... drive delivery with processIncoming() ...

$js->stopOrderedConsumer($sid)->await();
```

Object names are base64url-encoded into the metadata subject, so a watch pattern is either `>` (all
objects) or one exact name. A pattern containing `*` or `>` in any other position is rejected,
because it can never match an encoded name. If an object's own name legitimately contains those
characters, pass `exactName: true` to watch it:

```php
$sid = $store->watch($handler, 'report>2024', exactName: true)->await();
```

### Object Store Streaming to Callback

> 📄 **Runnable example:** [`examples/object-store-streaming-to-callback.php`](examples/object-store-streaming-to-callback.php)

_Verified by: [ObjectStoreBucketTest](tests/Unit/ObjectStoreBucketTest.php) (`testGetToCallbackInvokesCallbackOncePerChunk`, `testGetToCallbackInvokesCallbackOnceForSingleChunkObject`); [features/jetstream-data/object_store.feature](features/jetstream-data/object_store.feature)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$store = $client->jetStream()->objectStore('assets');
$store->create()->await();
$store->put('logo.txt', 'hello-object')->await();

// getToCallback streams the object chunk-by-chunk: the callback is invoked once per stored
// chunk as it is downloaded (the whole object is never buffered in memory), and the SHA-256
// digest is verified incrementally after the final chunk.
$info = $store->getToCallback('logo.txt', static function (string $chunk): void {
	echo $chunk;
})->await();

echo PHP_EOL;
echo $info?->name . PHP_EOL;

$store->deleteBucket()->await();
$client->disconnect()->await();
```

### Object Store Streaming Upload

> 📄 **Runnable example:** [`examples/object-store-streaming-upload.php`](examples/object-store-streaming-upload.php)

_Verified by: [ObjectStoreBucketTest](tests/Unit/ObjectStoreBucketTest.php) (`testPutStreamReChunksAndComputesDigestIncrementally`, `testPutStreamReChunksLargeBlockAcrossManyChunks`); [JetStreamIntegrationTest](tests/Integration/JetStreamIntegrationTest.php) (`testJetStreamObjectStorePutStreamRoundTrip`)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$store = $client->jetStream()->objectStore('assets');
$store->create()->await();

// putStream() pulls the object's bytes from a producer callback (return the next block, or null at
// end of stream), so the whole payload is never held in memory. Blocks of any size are re-chunked to
// the bucket's chunk size, published in bounded in-flight windows, and the SHA-256 digest is computed
// incrementally - the streaming counterpart to getToCallback().
$handle = fopen('/path/to/large.bin', 'rb');
$info = $store->putStream('large.bin', static function () use ($handle): ?string {
	$block = fread($handle, 1 << 16);

	return ($block === '' || $block === false) ? null : $block;
})->await();
fclose($handle);

echo $info->size . ' bytes in ' . $info->chunks . ' chunks' . PHP_EOL;

$store->deleteBucket()->await();
$client->disconnect()->await();
```

### Services Framework

> 📄 **Runnable example:** [`examples/services-framework.php`](examples/services-framework.php)

_Verified by: [ServiceTest](tests/Unit/ServiceTest.php); [NatsClientIntegrationTest](tests/Integration/NatsClientIntegrationTest.php) (`testServiceDiscoveryAndEndpoint`, `testServiceMultipleEndpoints`, `testServiceGroupedEndpointsHierarchy`, `testServiceEndpointsLoadBalanceAcrossInstances`); [features/services/](features/services/)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;

$serviceClient = new NatsClient(new NatsOptions());
$serviceClient->connect()->await();

$service = $serviceClient->service('echo', '1.0.0', 'Echo demo')
	->addEndpoint('echo', 'svc.echo', static function (NatsMessage $message): string {
		return 'reply:' . $message->payload;
	});

// Handlers can also be provided as objects implementing
// IDCT\NATS\Services\ServiceEndpointHandlerInterface or class-string adapters.

$service->addGroup('svc')->addGroup('v1')->addEndpoint(
	'echo-v1',
	'echo',
	static function (NatsMessage $message): string {
		return 'v1:' . $message->payload;
	},
);

$service->start()->await();

// In another client you can call discovery or endpoint subjects:
// - $SRV.PING.echo
// - $SRV.INFO.echo
// - $SRV.STATS.echo
// - $SRV.SCHEMA.echo
// - svc.echo

$service->stop()->await();
$serviceClient->disconnect()->await();

// Optional runtime helper: start + process loop + auto-stop on timeout.
// $service->run(timeoutSeconds: 30.0)->await();
```

Services expose runtime helpers: `statsSnapshot()` returns the current `io.nats.micro.v1.stats_response` array (per-endpoint `num_requests` / `num_errors` / `last_error` / `processing_time` / `average_processing_time`), `reset()` zeroes those counters, and `withRequestValidator()` enables opt-in per-request validation for endpoints declared with a `schema`. The validator receives the message and the endpoint schema and returns `null` to accept or a string rejection reason (which becomes a `VALIDATION_ERROR` error reply). A validator that throws is answered like a handler that throws: the requester gets a `HANDLER_ERROR` reply without the exception's text, which the endpoint keeps as its `last_error` and counts in `num_errors` (a `ServiceError` it throws is sent as chosen).

_Verified by: [ServiceTest](tests/Unit/ServiceTest.php) (`testStatsIncludeDetailedMetrics`, `testResetClearsStats`, `testRequestValidatorCanRejectRequests`, `testRequestValidatorThatThrowsIsAnsweredLikeAHandlerThatThrows`, `testRequestValidatorThatThrowsAServiceErrorGetsTheReplyItChose`)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$service = $client->service('echo', '1.0.0')
	->withRequestValidator(static function (NatsMessage $message, array $schema): ?string {
		// Return null to accept, or a string describing why the request is rejected.
		return $message->payload === '' ? 'payload must not be empty' : null;
	})
	->addEndpoint('echo', 'svc.echo', static fn (NatsMessage $message): string => $message->payload, schema: ['type' => 'object']);

$service->start()->await();

// Inspect live counters.
$stats = $service->statsSnapshot();
echo $stats['endpoints'][0]['num_requests'] . PHP_EOL;

// Zero the runtime statistics.
$service->reset();

$service->stop()->await();
$client->disconnect()->await();
```

`run()` reads the client's shared connection, so its loop delivers the messages of every subscription on it,
not only the service's. A handler that throws during such a read - another subscription's - is reported
through the `errorListener` and logged at error level, the messages the read brought behind it are still
delivered, and the loop serves on without backing off. The same goes for a handler that throws a
`CancelledException`: only `run()`'s own timeout or cancellation stops the loop, or a connection closed for
good. A handler that awaits the very cancellation you pass to `run()`, and is still waiting when it fires,
therefore reports that `CancelledException` too. An endpoint handler's own exception never gets this far: it
becomes a `HANDLER_ERROR` reply. The loop's read also delivers what another read has queued and not yet
reached, instead of waiting for the server to send more: the messages behind a handler that awaits in
another fiber's delivery, or the rest of a subscription whose handler threw in your own `processIncoming()`
(see [Handler Failures](#handler-failures)). Not while a `disconnect()` is closing the connection: that is
the close's to discard. A read that receives anything during the close, the loop's or any other, still
delivers it with what it received. `drain()` reports such a handler as well, and its flush reads on to the
`PONG` that confirms the server has processed the `UNSUB`s.

The read of an operation - a `request()`, a JetStream call, a polling queue - reports another
subscription's handler that throws in the same way, delivers the rest and completes (see
[Handler Failures](#handler-failures)), so a `request()` that an endpoint handler makes gets its reply even
when its read delivers to such a handler. With `handlerErrorsFailOperations: true` it fails with that
handler's exception instead, as before that option existed, and the endpoint then answers its requester with
a `HANDLER_ERROR` reply and records that exception as its `last_error`.

_Verified by: [ServiceTest](tests/Unit/ServiceTest.php) (`testRunReportsASubscriptionHandlerThatThrowsAndReadsOn`, `testRunAnswersARequestReadBehindAMessageWhoseHandlerThrows`, `testRunDoesNotBackOffAfterAHandlerFails`, `testRunReportsAHandlerThatThrowsACancelledExceptionAndServesOn`, `testARequestAnotherFibersReadBringsBehindAThrowingHandlerIsAnsweredBeforeThatReadFails`, `testRunAnswersARequestTheReplayAfterAReconnectBroughtBehindAThrowingHandler`, `testRunStoppedWhileAHandlerAwaitsTheSameCancellationReportsThatHandlersCancelledException`, `testDrainReportsAHandlerThatThrowsDuringItsFlushAndReadsOnToItsPong`), [HandlerFailureDuringOperationTest](tests/Unit/HandlerFailureDuringOperationTest.php) (`testARequestAServiceEndpointMakesCompletesDespiteAnotherSubscriptionsHandlerFailure`), [ServerErrorKeepingConnectionTest](tests/Unit/ServerErrorKeepingConnectionTest.php) (`testYourOwnReadThrowsAfterDeliveringTheOtherSubscriptionsAndAServingReadDeliversTheRemainder`, `testAReadForAServingLoopThatWaitedForAnotherFibersReadDeliversWhatThatReadHasNotReached`), [WaitForReconnectTest](tests/Unit/WaitForReconnectTest.php) (`testAServingReadDeliveringWhatAnotherReadHasNotReachedLeavesTheHandshakeToTheRecovery`), [CloseIntentTest](tests/Unit/CloseIntentTest.php) (`testServingReadDuringADisconnectLeavesWhatAnEarlierReadLeftQueuedToTheClose`), [ReconnectedListenerTest](tests/Unit/ReconnectedListenerTest.php) (`testMessageTheReconnectReadIsNotDeliveredByAServiceLoopWhileTheListenersDisconnectIsUnderWay`, `testMessageTheReconnectReadIsNotDeliveredByAServiceLoopThatWaitedForTheReconnectWhenTheListenerAwaitsItsDisconnect`)._

### Services: SCHEMA Discovery

> 📄 **Runnable example:** [`examples/services-schema-discovery.php`](examples/services-schema-discovery.php)

_Verified by: [ServiceTest](tests/Unit/ServiceTest.php) (schema validation + lifecycle observers), [BasicJsonSchemaValidatorTest](tests/Unit/BasicJsonSchemaValidatorTest.php); [NatsClientIntegrationTest::testServiceStatsAndObserversWithHeaders](tests/Integration/NatsClientIntegrationTest.php); [features/services/service_discovery.feature](features/services/service_discovery.feature)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Services\BasicJsonSchemaValidator;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$service = $client->service('calc', '1.0.0', 'Calculator')
	->withSchemaValidator(new BasicJsonSchemaValidator())
	->addObserver(static function (string $event, $endpoint, NatsMessage $message, array $context): void {
		// Example events: request_start, request_error, request_end
		// Example context key: correlation_id (from X-Request-Id/traceparent headers)
	})
	->addEndpoint('add', 'calc.add', static function (NatsMessage $message): string {
		return 'result';
	}, schema: [
		'type' => 'object',
		'required' => ['a', 'b'],
		'properties' => [
			'a' => ['type' => 'integer'],
			'b' => ['type' => 'integer'],
		],
	]);

$service->start()->await();

// Another client can discover the schema:
// $reply = $client->request('$SRV.SCHEMA.calc', '')->await();
// The response includes endpoint schemas in the JSON payload.
// Invalid request payloads receive a structured envelope:
// {"type":"io.nats.micro.v1.error","code":"VALIDATION_ERROR","message":"...","error":"...","correlation_id":"..."}

$service->stop()->await();
$client->disconnect()->await();
```

### Graceful Drain

> 📄 **Runnable examples:** [`examples/graceful-drain.php`](examples/graceful-drain.php), [`examples/pull-consumer-graceful-drain.php`](examples/pull-consumer-graceful-drain.php)

_Verified by: [NatsConnectionTest](tests/Unit/NatsConnectionTest.php) (`testDrainUnsubscribesAllAndCloses`, `testDrainDeliversBufferedMessagesBeforeClosing`), [DrainLifecycleTest](tests/Unit/DrainLifecycleTest.php), [PullConsumerClientDrainTest](tests/Unit/PullConsumerClientDrainTest.php), [PullConsumerConnectionLossTest::testAnApplicationCloseDiscardsWhatThePullHoldsOnADisconnectAndHandsItOverOnADrain](tests/Unit/PullConsumerConnectionLossTest.php); live: [DrainLifecycleIntegrationTest](tests/Integration/DrainLifecycleIntegrationTest.php); [features/resilience/client_resilience.feature](features/resilience/client_resilience.feature)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$client->subscribe('events.>', static function (NatsMessage $message): void {
	echo $message->payload . PHP_EOL;
})->await();

// Gracefully drain: unsubscribes all SIDs, delivers pending messages, then closes.
$client->drain()->await();
```

Note that `disconnect()` and a plain `unsubscribe($sid)` discard any locally queued,
undelivered messages (nats.go `Close()`/`Unsubscribe()` parity) - messages already received
from the server but not yet dispatched to a handler are dropped without warning. Use
`drain()` (whole connection) or `drainSubscription($sid)` (single subscription) as the
lossless teardown paths: both deliver the buffered backlog first. `drain()` also delivers what a JetStream
pull consumer run holds (#207). The run hands its handler a pull's messages only when the pull completes,
so its pulls can hold messages the connection received long before: once its flush is done, `drain()` asks
each run to hand its pulls over, in order, while the connection is Draining, and waits for that, your
handler included, within its budget. The acks your handler publishes (`ack()`, `nak()`, `term()`,
`inProgress()`) still go out then, and so do its requests (#213): `ackSync()` is confirmed, and a
JetStream publish, a Key/Value read or write, or a `request()` or `requestMany()` to a service gets its
reply, also when the worker has made no request before. The run then ends,
and its `handle()` resolves with the count, as after the iterator's `PullConsumerIterator::drain()`; while
the client drains, a run issues no new pull. Code the run calls (its handler, its `onError`, the
`errorListener` or logger while the run reports to them) that awaits the client's `drain()` holds that
drain for its whole budget, as a subscription handler that awaits it does: the drain waits for the run,
which goes on only once that call returns, and the rest of the pull is then discarded. Stop or drain the
iterator there and drain the client once `handle()` has resolved, or call the client's `drain()` from
another fiber, such as a signal handler, or without awaiting it. When the budget runs out first, `drain()`
closes the connection all the same: what the run still holds is discarded from then on, also while the
connection is being closed, and the "drain deadline exceeded" error below counts it. `disconnect()`
discards what a run holds wherever the run is, as it discards the connection's own queue: a handler in the
middle of a pull, also one that calls `disconnect()` itself, gets none of the rest, which stays unacked
for the server to deliver again after the ack wait (lost on a consumer without acks). `handle()` then
fails, with `Connection is not open` or the error of the read or write that met the close, except where
the client's `drain()` had already asked the run for its hand-over, or where a finite run has no pull left
to issue: it then resolves with its count. A `drain()` that finds no connection to drain (its budget ran
out before the reconnect it waited for was done, or that reconnect gave up) asks no run, and what the run
holds is discarded too. The client's `drain()` used to close the connection under the run and drop what
its pulls held (see [Pull Consumer Batching/Iteration](#pull-consumer-batchingiteration)).

`drain()` is bounded by roughly one `requestTimeoutMs` budget and always reaches the `Closed` state. It
ends as soon as the server answers its flush, also when another fiber - a `processIncoming()` loop, a
service's `run()`, a `request()` issued just before - reads the connection and takes that answer, and
the pull consumer runs it asked have handed over what they hold (see above). It does
not throw when a write fails against a dead socket: that failure is reported through the `errorListener`
and the teardown continues, so the connection is never stranded mid-drain. A handler that throws while
the drain delivers is reported the same way, and the messages behind it - already queued, or still
arriving during the drain's flush - are still delivered. So is an `-ERR` the server keeps the connection
open for (`maximum subscriptions exceeded`, say; see [Reconnect Behavior](#reconnect-behavior)) during the
flush: it is reported, and the flush reads on to its `PONG`, so the messages behind it are delivered too,
or the drain waits out its budget when no `PONG` comes; a fatal `-ERR` ends the flush.
Publishes your handlers issue while the drain is running share the same remaining budget rather than each
getting a fresh timeout, and anything still buffered when the budget runs out is dropped with a "drain
deadline exceeded" error naming the count. If you need the backlog delivered under all circumstances,
drain earlier rather than relying on a longer timeout.

Requests work while the drain delivers (#213). The drain keeps the shared reply inbox subscribed until it
has delivered everything - its own backlog, what its flush brings, what the pull consumer runs hand over -
subscribes it for a connection's first request, and unsubscribes it once that delivery phase is over, so
a handler it runs can make requests: `ackSync()`, a JetStream publish, a Key/Value read or write,
`request()` and `requestMany()`. Each request keeps its own timeout and its caller's cancellation, and
also ends with the delivery phase: once everything is delivered, or at the drain's deadline, a request
still waiting fails with `Connection is not open` (a `requestMany()` returns what it has collected), so
the drain still closes within its one budget. From the deadline on no handler gets another message, the
first of a new delivery pass included, and what is left is counted for the "drain deadline exceeded"
report where it is at that moment, before the requests still waiting end; a drain that found no
connection to drain still hands the first message of its final backlog pass over, with requests refused.
The round trips count against that budget: a hundred held messages acked with `ackSync()` at a 100 ms
round trip take all of the default 10 s. A request made by another fiber while the drain delivers is
sent as well, but the drain does not wait for it: one still waiting when the delivery phase ends fails
with `Connection is not open` although the server may have acted on it, so give a JetStream publish made
there a `msgId`. A request made after that phase, after a `disconnect()` that interrupts the drain, or
from the listener the "drain deadline exceeded" report goes to, fails with `Connection is not open`
without being sent. `subscribe()` stays refused while Draining, and so do the operations that need a
subscription of their own: `fetchBatch()`, `fetchNext()`, a pull consumer's new pulls, batched Direct
Get, Key/Value `keys()` and `history()`, and an Object Store download of more than one chunk. Before
2.24.4 every request failed at once with `Connection is not open` while Draining, so an `ackSync()`
handler processed every message the drain handed over twice: once then, and again after the ack wait.
`disconnect()` ends the requests in flight as it begins, wherever they wait, rather than once the close
of the transport fails their reads. _Verified by: [RequestsDuringDrainTest](tests/Unit/RequestsDuringDrainTest.php),
[PullConsumerClientDrainTest](tests/Unit/PullConsumerClientDrainTest.php) (`testAnAckSyncDuringTheDrainsHandOverIsConfirmed`,
`testAJetStreamPublishAKeyValueReadAndARequestManyDuringTheDrainsHandOverGetTheirReplies`,
`testARequestOutlastingTheDrainsBudgetEndsWithTheDeliveryPhase`); live:
[DrainLifecycleIntegrationTest](tests/Integration/DrainLifecycleIntegrationTest.php)
(`testAnAckSyncDuringTheDrainsHandOverIsConfirmedAgainstALiveServer`)._

A `drain()` issued while the client is reconnecting first waits for the reconnect within that same
budget: the reconnect flushes the publishes buffered during the outage, then the new connection is
drained as usual. The drain goes on as soon as the reconnect is over, without waiting for its
`Reconnected` listener or for a logger recording it. A `publish()` whose failed write ran that reconnect
is retried only after both, unless the listener is called from the event loop (see item 8 of
[Reconnect Behavior](#reconnect-behavior)), so it can find the connection drained and closed by then: it
fails with the error of its write (a built-in transport's raw stream error wrapped in a
`TransportClosedException`), and its frame is not sent. One retried while the drain is still flushing is
written to the connection being drained. If the budget runs out first, the drain still reaches `Closed` -
the reconnect is stopped, the backlog pass runs, and the buffered publishes it discards are reported
through the `errorListener`. A drain that cannot wait - `waitForReconnect: false`, or a drain called from a
listener that runs inside the reconnect, such as a `Disconnected` listener - closes the connection and
throws, like nats.go's `Drain()` while reconnecting, so the reconnect cannot reopen a connection you are
shutting down. A `Reconnected` listener runs once the reconnect is over, so a drain it calls is an
ordinary drain. See [Reconnect Behavior](#reconnect-behavior).

Every `drain()` ends with one `Closed` event. When the drain closes the connection itself, the event
comes once the drain is over, so a supervisor that reconnects on `Closed` can call `connect()` straight
from its listener. When something else closed it first - a reconnect that gave up, or a `disconnect()`
during the drain - that path's `Closed` is the only one. While a drain runs, including while it waits
for a reconnect, `connect()` is refused with `Cannot connect: drain in progress`, because the drain's
teardown would close a connection opened meanwhile. A `connect()` from a `Closed` listener also fails
at once (`Recovery was aborted before the connection opened`) while a reconnect the close stopped is
still winding down - one `Disconnected` listener awaiting the drain from inside that reconnect is enough -
so a supervisor should retry shortly rather than give up. `disconnect()` after `drain()` closes quietly: the
close is announced once.

`drainSubscription($sid)` unsubscribes one subscription, flushes, and delivers what already arrived for
it before removing it, all within one `requestTimeoutMs` budget; the UNSUB write is bounded by it too,
so a peer that stopped reading cannot hold the call. While the client is reconnecting it waits for the
reconnect within that budget, like `flush()`, and then drains the subscription on the new connection. A
reconnect never re-subscribes a subscription that is being drained - but one that had already
re-subscribed it keeps it until just before it goes live, and the server can still deliver on it until
then (a queue group's share, say); draining on the new connection delivers those messages too. When it
cannot wait - `waitForReconnect: false`, called from a listener inside the reconnect, or the budget ran out
- it delivers what already arrived and removes the subscription; messages the server sends on a
subscription the reconnect already re-subscribed are then dropped until the reconnect's UNSUB lands.
During a `drain()` the drain takes the subscription over: it has already unsubscribed it, its flush may
still bring messages for it, and it delivers those and what is queued before it removes every
subscription, so `drainSubscription()` resolves at once. Like `drain()`, it does not throw for a failed
UNSUB or flush or for a handler that throws: those are reported through the `errorListener`, and a
throwing handler does not cost the messages behind it, whether already queued or still arriving during
the flush. Its flush reads on past an `-ERR` the server keeps the connection open for, too, and reports
it. A connection lost during the flush - the server closing it right after such an `-ERR`, say - is
noticed by the flush's own read, which starts the reconnect; that runs on in a fiber of its own, and the
flush fails as soon as its first attempt ends the lost connection's `PONG`s, with `Connection lost before
the server answered the PING` (at once, with `Connection is not open`, when `waitForReconnect` is `false`),
so the call reports that and removes the subscription while the reconnect runs on. When a delivery for
the subscription is already under way - on another fiber, or it is your
handler draining its own subscription - that delivery hands over the messages queued behind it and then
removes the subscription, and `drainSubscription()` resolves without waiting for it. A second call for a
subscription that is already being drained resolves at once.

_Verified by: [DrainLifecycleTest](tests/Unit/DrainLifecycleTest.php) (`testDrainFlushReportsAnErrTheServerKeepsTheConnectionOpenForAndReadsOnToTheMessagesStillInFlight`, `testDrainFlushReadsOnPastANonClosingErrButEndsAtAFatalOneAfterIt`, `testDrainSubscriptionWhoseFlushMeetsAnErrAndThenACloseStartsTheReconnectAndResolvesBeforeItGivesUp`, `testDrainEndsPromptlyWhileAnotherFiberReads`, `testDrainIssuedRightAfterARequestEndsPromptly`, `testFlushEndsPromptlyWheneverAnotherFibersReadStartsAroundIt`, `testDrainEndsPromptlyAlongsideALoopThatLeftTheRestOfAFailingSubscriptionQueued`, `testADrainEndsPromptlyWhileAServiceLoopDeliversARead`)._

### Ordered Consumer

> 📄 **Runnable example:** [`examples/ordered-consumer.php`](examples/ordered-consumer.php)

_Verified by: [JetStreamContextTest](tests/Unit/JetStreamContextTest.php) (`testSubscribeOrderedConsumerSendsCorrectConfig`, `testSubscribeOrderedConsumerRecreatesOnSequenceGap`, `testSubscribeOrderedConsumerDeliversFilteredMessagesWithoutSpuriousRecreate`); [JetStreamIntegrationTest::testJetStreamOrderedConsumerWithFilteredSubjectAfterPriorMessages](tests/Integration/JetStreamIntegrationTest.php); [features/jetstream-core/consumer_helpers.feature](features/jetstream-core/consumer_helpers.feature)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$js = $client->jetStream();
$js->createStream('EVENTS', ['events.>'])->await();

// Ordered consumer: ephemeral push consumer with flow control,
// idle heartbeat, and ack_policy=none for ordered delivery.
$sid = $js->subscribeOrderedConsumer(
	stream: 'EVENTS',
	handler: static function (NatsMessage $message): void {
		echo $message->payload . PHP_EOL;
	},
	filterSubject: 'events.>',
)->await();

$js->publish('events.order', '{"id":1}')->await();
$client->processIncoming()->await();

$js->stopOrderedConsumer($sid)->await(); // resolves the CURRENT sid even after gap-driven recreates rotated it
$js->deleteStream('EVENTS')->await();
$client->disconnect()->await();
```

`subscribeOrderedConsumer()` takes two further optional arguments. `consumerOverrides` merges extra
consumer configuration into the created instance, for example a deliver policy; a recreate before the
first delivery re-applies that same initial policy, so a `new` or `last_per_subject` consumer never
replays the whole stream. `onConsumerCreated` is invoked once with the INITIAL instance's
`ConsumerInfo` (not on recreates), which is how a watch inspects `num_pending` (via the raw payload)
to tell that it started with nothing pending:

```php
$sid = $js->subscribeOrderedConsumer(
	stream: 'EVENTS',
	handler: $handler,
	filterSubject: 'events.>',
	idleHeartbeatNs: 5_000_000_000,
	consumerOverrides: ['deliver_policy' => 'last_per_subject'],
	onConsumerCreated: static function (ConsumerInfo $info): void {
		echo 'initial instance ' . $info->name . ', pending: ' . ($info->raw['num_pending'] ?? 0) . PHP_EOL;
	},
)->await();
```

Always stop an ordered consumer with `stopOrderedConsumer($sid)`. A recreate rotates the internal
subscription id, so a plain `unsubscribe($sid)` only works until the first recreate, and it leaves
the server-side ephemeral consumer to expire on its own instead of deleting it.

### Consumer Pause/Resume

> 📄 **Runnable example:** [`examples/consumer-pause-resume.php`](examples/consumer-pause-resume.php)

_Verified by: [JetStreamContextTest](tests/Unit/JetStreamContextTest.php) (`testPauseConsumerSendsCorrectPayload`, `testResumeConsumerSendsEmptyBody`); [JetStreamIntegrationTest::testJetStreamPauseAndResumeConsumer](tests/Integration/JetStreamIntegrationTest.php); [features/jetstream-core/consumer_helpers.feature](features/jetstream-core/consumer_helpers.feature)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$js = $client->jetStream();
$js->createStream('ORDERS', ['orders.>'])->await();
$js->createConsumer('ORDERS', 'PROC', 'orders.created')->await();

// Pause the consumer until a specific time (ISO 8601 format). A time in the past resumes it
// immediately, so build the instant relative to now rather than hardcoding a date.
$until = (new DateTimeImmutable('+1 hour', new DateTimeZone('UTC')))->format(DATE_RFC3339);
$js->pauseConsumer('ORDERS', 'PROC', $until)->await();

// Resume the consumer immediately.
$js->resumeConsumer('ORDERS', 'PROC')->await();

$js->deleteConsumer('ORDERS', 'PROC')->await();
$js->deleteStream('ORDERS')->await();
$client->disconnect()->await();
```

### Fetch Batch

> 📄 **Runnable example:** [`examples/fetch-batch.php`](examples/fetch-batch.php)

_Verified by: [JetStreamContextTest](tests/Unit/JetStreamContextTest.php) (`testFetchBatch`, `testFetchBatchIgnoresTerminalStatusFrames`, `testFetchBatchThrowsWhenNoMessagesArrive`); [JetStreamIntegrationTest::testJetStreamFetchBatchHandlesStatusFrames](tests/Integration/JetStreamIntegrationTest.php); [features/jetstream-core/consumer_helpers.feature](features/jetstream-core/consumer_helpers.feature)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$js = $client->jetStream();
$js->createStream('LOGS', ['logs.>'])->await();
$js->createConsumer('LOGS', 'BATCH', 'logs.>')->await();

for ($i = 0; $i < 5; $i++) {
	$js->publish('logs.app', "log entry $i")->await();
}

// Fetch up to 5 messages in one batch.
$messages = $js->fetchBatch('LOGS', 'BATCH', batch: 5, expiresMs: 3000)->await();
foreach ($messages as $message) {
	$js->ack($message)->await();
}

$js->deleteConsumer('LOGS', 'BATCH')->await();
$js->deleteStream('LOGS')->await();
$client->disconnect()->await();
```

Notes:
1. A partial batch is valid. If the server delivers some messages and then ends the pull with a terminal status, the delivered messages are returned.
2. A terminal status only becomes an exception when no user message was delivered for that pull request.
3. The fetch's `expiresMs + 1000` ms budget starts before subscribing its inbox and covers reconnect,
   SUB/PUB backpressure, failed-write recovery and collection. An empty deadline is the usual
   `JetStreamException` with code 408 and message `No messages received within timeout`; a partial
   batch is still returned. A delayed send shortens server `expires` to fit the remaining budget,
   reserving 100 ms for its terminal response. A heartbeat that no longer fits the shortened expiry
   is omitted and its local miss check disabled. No new attempt starts after the operation ends;
   an already-started write can complete later. Inbox cleanup cannot overrun this budget (#188).
4. At the connection's subscription limit the fetch fails at once instead of reporting an empty batch after its expiry: with a `ConnectionException` naming its inbox when another read meets the server's `-ERR`, with the server's error when its own read does - see [JetStream Pull Consumer (Fetch + ACK)](#jetstream-pull-consumer-fetch--ack) (#175).

### Stream Purge and List

> 📄 **Runnable example:** [`examples/stream-purge-and-list.php`](examples/stream-purge-and-list.php)

_Verified by: [JetStreamContextTest](tests/Unit/JetStreamContextTest.php) (`testPurgeStream`, `testPurgeStreamWithSubjectFilter`, `testListStreams`); [JetStreamIntegrationTest](tests/Integration/JetStreamIntegrationTest.php) (`testJetStreamPurgeStreamByFilter`, `testJetStreamListStreams`); [features/jetstream-core/management.feature](features/jetstream-core/management.feature)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$js = $client->jetStream();
$js->createStream('LOGS', ['logs.>'])->await();
$js->publish('logs.app', 'entry 1')->await();
$js->publish('logs.app', 'entry 2')->await();

// Purge all messages from the stream.
$result = $js->purgeStream('LOGS')->await();
echo 'Purged: ' . $result['purged'] . PHP_EOL;

// Purge by subject filter.
// $js->purgeStream('LOGS', ['filter' => 'logs.app'])->await();

// List all streams.
$streams = $js->listStreams()->await();
foreach ($streams as $stream) {
	echo $stream->name . PHP_EOL;
}

$js->deleteStream('LOGS')->await();
$client->disconnect()->await();
```

### Consumer List

> 📄 **Runnable example:** [`examples/consumer-list.php`](examples/consumer-list.php)

_Verified by: [JetStreamContextTest::testListConsumers](tests/Unit/JetStreamContextTest.php); [JetStreamIntegrationTest::testJetStreamListConsumers](tests/Integration/JetStreamIntegrationTest.php); [features/jetstream-core/management.feature](features/jetstream-core/management.feature)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$js = $client->jetStream();
$js->createStream('ORDERS', ['orders.>'])->await();
$js->createConsumer('ORDERS', 'PROC_A', 'orders.created')->await();
$js->createConsumer('ORDERS', 'PROC_B', 'orders.updated')->await();

$consumers = $js->listConsumers('ORDERS')->await();
foreach ($consumers as $consumer) {
	echo $consumer->name . ' (push=' . ($consumer->push ? 'yes' : 'no') . ')' . PHP_EOL;
}

$js->deleteConsumer('ORDERS', 'PROC_A')->await();
$js->deleteConsumer('ORDERS', 'PROC_B')->await();
$js->deleteStream('ORDERS')->await();
$client->disconnect()->await();
```

### Stream Message Get

> 📄 **Runnable example:** [`examples/stream-message-get.php`](examples/stream-message-get.php)

_Verified by: [JetStreamContextTest](tests/Unit/JetStreamContextTest.php) (`testGetStreamMessage`, `testGetStreamMessagePreservesZeroPayload`, `testGetStreamMessageDecodesHeaders`); [JetStreamIntegrationTest](tests/Integration/JetStreamIntegrationTest.php) (`testJetStreamGetStreamMessage`, `testJetStreamGetStreamMessagePreservesZeroAndHeaders`)._

`getStreamMessage()` fetches a stored message by sequence using the standard JetStream
`$JS.API.STREAM.MSG.GET` API. The returned `NatsMessage` preserves the stored subject, payload
(including a body that is exactly `"0"`), and any stored headers on `rawHeaders`.

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$js = $client->jetStream();
$js->createStream('EVENTS', ['events.>'])->await();
$js->publish('events.order', '{"id":1}')->await();

// Fetch message by stream sequence number.
$message = $js->getStreamMessage('EVENTS', 1)->await();
echo $message->payload . PHP_EOL;

$js->deleteStream('EVENTS')->await();
$client->disconnect()->await();
```

Stored messages can be removed by sequence with `deleteMessage()`. By default this is a fast delete (`no_erase`: the sequence is unlinked but the bytes are left in place). Pass `secureErase: true` for a secure delete that overwrites the message data before removal (slower; mirrors nats.go `SecureDeleteMsg`).

_Verified by: [JetStreamContextTest::testDeleteMessageFastAndSecure](tests/Unit/JetStreamContextTest.php)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$js = $client->jetStream();

// Fast delete (default): unlink sequence 7, leave the bytes on disk.
$js->deleteMessage('ORDERS', 7)->await();

// Secure erase: overwrite the data for sequence 8 before removing it.
$js->deleteMessage('ORDERS', 8, secureErase: true)->await();

$client->disconnect()->await();
```

### JetStream Direct Get

> 📄 **Runnable example:** [`examples/jetstream-direct-get.php`](examples/jetstream-direct-get.php)

`directGetStreamMessage()` and `directGetLastMessageForSubject()` use the JetStream Direct Get API
(`$JS.API.DIRECT.GET`), which requires the stream to be created with `allow_direct: true`. Unlike
`getStreamMessage()` (regular `$JS.API.STREAM.MSG.GET`, served by the stream leader), Direct Get can
be answered by any replica. The server returns the stored message directly: the payload is the raw
body and the stream/sequence/subject/timestamp travel as `Nats-*` headers (preserved on `rawHeaders`).
A miss is raised as a `JetStreamException` (for example `404 Message Not Found`).

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$js = $client->jetStream();
$js->createStream('EVENTS', ['events.>'], ['allow_direct' => true])->await();
$js->publish('events.order', '{"id":1}')->await();

// Direct Get by stream sequence.
$bySeq = $js->directGetStreamMessage('EVENTS', 1)->await();
echo $bySeq->subject . ': ' . $bySeq->payload . PHP_EOL;

// Direct Get the last message stored on a subject.
$last = $js->directGetLastMessageForSubject('EVENTS', 'events.order')->await();
echo $last->payload . PHP_EOL;

$js->deleteStream('EVENTS')->await();
$client->disconnect()->await();
```

_Verified by: [JetStreamContextTest](tests/Unit/JetStreamContextTest.php) (`testDirectGetStreamMessageReturnsRawBodyAndHeaders`, `testDirectGetLastMessageForSubjectRequestsLastBySubj`, `testDirectGetStreamMessageThrowsOnNotFound`); [JetStreamIntegrationTest::testJetStreamDirectGetStreamMessage](tests/Integration/JetStreamIntegrationTest.php)._

#### Batched / multi Direct Get

For multi-message reads the client adds two batched Direct Get helpers (ADR-31), which **require NATS
server 2.11+** in addition to the stream's `allow_direct`. Each issues a single request whose replies
stream to a private inbox, terminated by a 204 end-of-batch marker (or a final message carrying
`Nats-Num-Pending: 0`); a silent server is bounded by a no-progress interval of `$expiresMs + 1000` ms
(`$expiresMs` defaults to 5000). That interval starts before
inbox setup, covering reconnect and SUB/PUB backpressure. Each data reply renews the same interval,
including replies another reader dispatches while PUB completion is pending. A healthy batch may
therefore take longer than one interval; an incomplete batch with no progress throws its stall
error rather than returning a truncated prefix. The multi-subject helper applies the interval to
separate chunks. Cleanup is bounded by the current interval (#188).
`directGetBatch()` returns a `list<NatsMessage>`; an error status (e.g. `408`) is
raised as a `JetStreamException`.

At the connection's subscription limit the batch fails at once instead of reporting a stalled batch
after `$expiresMs` plus a second: with a `ConnectionException` naming its inbox (`Direct Get batch for
stream "<stream>" failed: the server may have rejected its reply-inbox subscription
"_INBOX.JS.DGET.<nuid>" because the connection is at its subscription limit (maximum subscriptions
exceeded). A retry subscribes a new one.`) when another read meets the server's `-ERR`, with the
server's error when its own read does. The inbox is confirmed by a `PING` behind its `SUB`, as a pull
fetch's is, under the same rules - several inboxes unconfirmed at once are all treated as rejected
(see [JetStream Pull Consumer (Fetch + ACK)](#jetstream-pull-consumer-fetch--ack), #175).

- `directGetLastForSubjects(string $stream, array $subjects, int $expiresMs = 5000)` fetches the
  latest message for each named subject in one round trip (`multi_last`). It expects **exact**
  subjects: a subject containing `*` or `>` is rejected with a `JetStreamException`. An empty
  `$subjects` array returns `[]` without contacting the server.
- `directGetBatch(string $stream, array $body, int $expiresMs = 5000)` issues a raw batched request;
  `$body` accepts keys such as `batch`, `seq`, `up_to_seq` and `multi_last`. `$expiresMs` must be
  greater than zero.

```php
$js = $client->jetStream();
$js->createStream('EVENTS', ['events.>'], ['allow_direct' => true])->await();
$js->publish('events.order', '{"id":1}')->await();
$js->publish('events.user', '{"id":2}')->await();

// Latest message per subject, in a single request.
$latest = $js->directGetLastForSubjects('EVENTS', ['events.order', 'events.user'])->await();
foreach ($latest as $msg) {
	echo $msg->subject . ': ' . $msg->payload . PHP_EOL;
}

// Raw batched get over a sequence range (here: up to 10 messages from sequence 1).
$range = $js->directGetBatch('EVENTS', ['seq' => 1, 'batch' => 10])->await();
echo count($range) . ' messages' . PHP_EOL;
```

_Verified by: [JetStreamContextTest](tests/Unit/JetStreamContextTest.php) (`testDirectGetBatchCollectsUntilEob`, `testDirectGetLastForSubjects`, `testDirectGetBatchSurfacesError`, `testDirectGetLastForSubjectsWithEmptySubjectsReturnsEmpty`, `testDirectGetLastForSubjectsRejectsWildcardSubjectWithStar`, `testDirectGetLastForSubjectsRejectsWildcardSubjectWithGreaterThan`, `testDirectGetBatchRejectsZeroExpiresMs`); [JetStreamInboxRejectionTest](tests/Unit/JetStreamInboxRejectionTest.php) (`testADirectGetBatchWhoseInboxSubIsRejectedWhileAnotherReadBringsTheErrFailsAtOnce`, `testAPermissionsViolationNamingTheInboxFailsTheOperationAtOnce`); [JetStreamIntegrationTest::testJetStreamBatchedDirectGet](tests/Integration/JetStreamIntegrationTest.php)._

### Atomic Batch Publish

> 📄 **Runnable example:** [`examples/atomic-batch-publish.php`](examples/atomic-batch-publish.php)

_Verified by: [BatchPublisherTest](tests/Unit/BatchPublisherTest.php) (`testCommitSendsBatchHeadersAndParsesAck`, `testCommitRejectedAtStart`, `testCommitAbortSurfacesError`, `testCommitEmptyBatchThrows`, `testBatchRejectsOversizedId`, `testAddAfterCommitThrows`, `testCountReturnsNumberOfStagedMessages`, `testBatchIdReturnsConstructedId`); [JetStreamIntegrationTest::testJetStreamAtomicBatchPublish](tests/Integration/JetStreamIntegrationTest.php)._

`$js->batch()` opens an atomic (all-or-nothing) JetStream publish batch (ADR-50). Messages are staged
with the fluent `add($subject, $payload, $headers = [])` and sent together on `commit()`, which returns
a `Future<PubAck>`. Every message carries a shared `Nats-Batch-Id` and an incrementing
`Nats-Batch-Sequence`; the final message carries `Nats-Batch-Commit: 1`, on which the server atomically
commits the whole batch and replies with a single `PubAck` whose `batchCount` is the committed count and
whose `batchId` echoes the batch id. If any consistency check fails the entire batch is aborted and
nothing is stored - the failure surfaces as a `JetStreamException`.

This is a NATS 2.12+ feature: the target stream must be created with `allow_atomic` enabled. On a
pre-2.12 server `createStream(..., ['allow_atomic' => true])` fails fast with an
`UnsupportedFeatureException` (see [NATS Server Version Requirements](#nats-server-version-requirements));
if the stream exists without `allow_atomic`, `commit()` surfaces a `JetStreamException` (e.g. "atomic
publish not enabled"). Pass your own batch id (1..64 characters) to `batch()`, or omit it to have one
generated. A single batch is capped at `BatchPublisher::MAX_MESSAGES` (1000) messages.

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$js = $client->jetStream();

// The stream must allow atomic batches (NATS 2.12+).
$js->createStream('ORDERS', ['orders.>'], ['allow_atomic' => true])->await();

// Stage messages, then commit them all atomically.
$batch = $js->batch()
	->add('orders.created', '{"id":1}')
	->add('orders.created', '{"id":2}')
	->add('orders.created', '{"id":3}');

echo $batch->count() . " messages staged in batch {$batch->batchId()}" . PHP_EOL;

$ack = $batch->commit()->await();

echo "Committed {$ack->batchCount} messages (batch {$ack->batchId})" . PHP_EOL;

$js->deleteStream('ORDERS')->await();
$client->disconnect()->await();
```

### Credentials File Authentication

> 📄 **Runnable example:** [`examples/auth-credentials-file.php`](examples/auth-credentials-file.php)

_Verified by: [CredentialsParserTest](tests/Unit/CredentialsParserTest.php) (`testParseAcceptsCanonicalNscMarkersWithSixDashEnd`, `testFromFileParsesRealNscFixtureWhenPresent`), [NkeySeedSignerTest](tests/Unit/NkeySeedSignerTest.php); [features/auth/jwt_and_nkey_auth.feature](features/auth/jwt_and_nkey_auth.feature)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Auth\CredentialsParser;
use IDCT\NATS\Auth\NkeySeedSigner;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

// Parse a .creds file to extract JWT and NKey seed.
$creds = CredentialsParser::fromFile('/path/to/user.creds');
$signer = new NkeySeedSigner($creds['nkeySeed']);

$client = new NatsClient(new NatsOptions(
	servers: ['nats://127.0.0.1:4222'],
	jwt: $creds['jwt'],
	nkey: $signer->publicKey(),
	nonceSigner: $signer,
));
```

### Typed Stream Configuration

> 📄 **Runnable example:** [`examples/typed-stream-configuration.php`](examples/typed-stream-configuration.php)

_Verified by: the [JetStream enums](src/JetStream/Enum/) are exercised end-to-end (all six via `->value`) in [features/jetstream-core/management.feature](features/jetstream-core/management.feature) and [JetStreamContextTest](tests/Unit/JetStreamContextTest.php)._

Stream and consumer configuration supports typed enums for type-safe options:

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\JetStream\Enum\AckPolicy;
use IDCT\NATS\JetStream\Enum\DeliverPolicy;
use IDCT\NATS\JetStream\Enum\DiscardPolicy;
use IDCT\NATS\JetStream\Enum\ReplayPolicy;
use IDCT\NATS\JetStream\Enum\RetentionPolicy;
use IDCT\NATS\JetStream\Enum\StorageBackend;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$js = $client->jetStream();

// Create stream with typed configuration.
$js->createStream('ORDERS', ['orders.>'], [
	'retention' => RetentionPolicy::Limits->value,
	'storage' => StorageBackend::Memory->value,
	'discard' => DiscardPolicy::Old->value,
	'max_msgs' => 100_000,
	'max_bytes' => 50 * 1024 * 1024,
	'max_age' => 86_400_000_000_000,  // 24h in nanoseconds
	'num_replicas' => 1,
	'duplicate_window' => 120_000_000_000,  // 2 min in nanoseconds
])->await();

// Create consumer with typed configuration.
$js->createConsumer('ORDERS', 'PROC', 'orders.created', [
	'deliver_policy' => DeliverPolicy::New->value,
	'ack_policy' => AckPolicy::Explicit->value,
	'replay_policy' => ReplayPolicy::Instant->value,
	'max_deliver' => 5,
	'max_ack_pending' => 1000,
	'ack_wait' => 30_000_000_000,  // 30s in nanoseconds
])->await();

$js->deleteConsumer('ORDERS', 'PROC')->await();
$js->deleteStream('ORDERS')->await();
$client->disconnect()->await();
```

### Pull Consumer Batching/Iteration

> 📄 **Runnable examples:** [`examples/pull-consumer-batching-iteration.php`](examples/pull-consumer-batching-iteration.php), [`examples/pull-consumer-graceful-drain.php`](examples/pull-consumer-graceful-drain.php), [`examples/pull-consumer-restart.php`](examples/pull-consumer-restart.php)

_Verified by: [PullConsumerIteratorTest](tests/Unit/PullConsumerIteratorTest.php); [PullConsumerWakeUpTest](tests/Unit/PullConsumerWakeUpTest.php); [PullConsumerConnectionLossTest](tests/Unit/PullConsumerConnectionLossTest.php); [PullConsumerConnectionEndingFrameTest](tests/Unit/PullConsumerConnectionEndingFrameTest.php); [PullConsumerClientDrainTest](tests/Unit/PullConsumerClientDrainTest.php); [PullConsumerReconnectTest](tests/Unit/PullConsumerReconnectTest.php); [PullConsumerOverflowTest](tests/Unit/PullConsumerOverflowTest.php); [JetStreamIntegrationTest::testJetStreamPullIteratorBatching](tests/Integration/JetStreamIntegrationTest.php); [JetStreamIntegrationTest::testAPullConsumerRestartedWithStopAndHandleInOneTickHandsWhatFollowsToTheNewHandler](tests/Integration/JetStreamIntegrationTest.php); [features/jetstream-core/consumer_helpers.feature](features/jetstream-core/consumer_helpers.feature)._

The fluent `PullConsumerIterator` drives the pipelined pull engine: it keeps up to `setDepth()` (default 2) pull round-trips in flight while your handler processes earlier ones, preserving message order, with configurable batch size, expiry, and iteration count. `setOnError()` receives errors the engine does not treat as routine, and `stop()` / `drain()` end the run from inside the handler or from another fiber (a signal handler's timer, a supervisor), ending the engine's wait on the socket at once; a `drain()` still lets the pulls already in flight complete first:

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\JetStream\JetStreamContext;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$js = $client->jetStream();

// Process messages in batches of 10, up to 5 iterations.
$totalProcessed = $js->pullConsumer('ORDERS', 'PROC')
	->setBatching(10)
	->setExpiresMs(5000)
	->setIterations(5)
	->handle(function (NatsMessage $msg, JetStreamContext $js): void {
		echo 'Processing: ' . $msg->payload . PHP_EOL;
		$js->ack($msg)->await();
	})->await();

echo "Processed {$totalProcessed} messages total." . PHP_EOL;

$client->disconnect()->await();
```

`stop()` and `drain()` act on the runs active when they are called, and each `handle()` run has stop and drain flags of its own (#189), so `stop()` followed by `handle()` restarts the consumer, in the same tick too, as a supervisor that changes the batch size on a signal does: the earlier run ends on its stop, its pull inbox released and whatever it holds left undelivered as after any `stop()`, and the new run starts clean, with the settings it was started with. After `drain()` and `handle()` the earlier run still delivers its pulls in flight to its own handler, next to the new run, and then ends without pulling again. The earlier run's handler may still be running when the new run starts, while it awaits an ack as another fiber restarts the consumer, say, or when the handler restarts its own consumer: await the earlier run's future first where two handlers must never run at once. Runs started on one iterator without a `stop()` in between all go on, sharing a priority group's pin, and one `stop()` ends them all, each at once. A `stop()` or `drain()` outside a run changes no later run. Up to 2.24.6 the runs of one iterator shared its two flags, so a `handle()` while a run was still active cleared a `stop()` or `drain()` that run had not seen yet: the earlier run went on next to the new one, with its own handler and settings, the two splitting the stream between them, until a later `stop()` reached it at its pull's deadline (its expiry plus a second).

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\JetStream\JetStreamContext;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$handler = function (NatsMessage $msg, JetStreamContext $js): void {
	$js->ack($msg)->await();
};

// An infinite run (no setIterations()).
$iterator = $client->jetStream()->pullConsumer('ORDERS', 'PROC')->setBatching(10);
$run = $iterator->handle($handler);

// Later, a supervisor reconfigures the consumer: the current run ends on its stop, and the
// new run pulls with the new batch size. Awaiting the stopped run first is optional.
$iterator->stop();
$newRun = $iterator->setBatching(3)->handle($handler);
$stoppedRunProcessed = $run->await();

// At shutdown, one stop() ends every run still active on the iterator.
$iterator->stop();
$newRun->await();

$client->disconnect()->await();
```

A run that fails hands your handler what its pulls have received before `handle()` throws (#197). The engine holds a pull's messages until the pull completes (its batch full, a status from the server, or its deadline), so a run can fail with messages the handler has not seen yet: when its read fails because the connection is going (lost with `waitForReconnect: false`, a reconnect that gave up, reconnect off, or a fatal `-ERR` or a `PONG` the socket would not take where the run does not go on past it, see below), when a pull's write fails (the reconnect it ran gave up, or reconnect is off), when the server rejects the run's reply inbox (see below), or when the read fails for a reason the options make its own (another subscription's handler with `handlerErrorsFailOperations`, an overflow with `slowConsumerErrorsFailOperations`). The handler gets them first, in the order they arrived, and `handle()` then throws that error unchanged, so you still learn why the run ended. The server counted those messages as delivered: dropped with the run, as they used to be, they came again only after the ack wait, or never on a consumer without acks (`ack_policy: none`) or with `max_deliver: 1`, which is also why `fetchBatch()` returns its partial batch when its read fails with the connection going. A `stop()` made before or during that delivery leaves the rest undelivered, as anywhere else, and so does a `disconnect()` before or during it, or a `drain()` of the client once its budget has run out: the close discards them, as `disconnect()` discards any message received and not yet handed to a handler (see [Graceful Drain](#graceful-drain)), rather than run your handler after the close has returned, with every ack failing. A `drain()` of the client within its budget does not: your handler gets them, and its acks go out (#207, see below). The iterator's `drain()` changes nothing for a run that fails: it lets the pulls in flight complete, and a failure meanwhile hands over what they hold all the same. The handler's acks go out as any publish does then: buffered while a reconnect is in flight (see [Reconnect Behavior](#reconnect-behavior), item 5), sent on a connection the failure left open, and failing on a closed one (reconnect off, a reconnect that gave up), the message then processed but left unacked, so the server delivers it again after the ack wait unless the consumer's `max_deliver` is reached. A handler that throws during that delivery, such as the one above when its ack fails on a closed connection, ends it: the rest is left undelivered and unacked, for the server to deliver again after the ack wait (never on a consumer without acks or with `max_deliver: 1`), and its exception goes to the `errorListener` and to the logger (`NATS connection error: ...`), since `handle()` throws the run's own failure, which came first. A handler that throws at any other time ends the run at once, as always, with its own exception and the other messages left undelivered.

An infinite run (no `setIterations()`) goes on when a frame ends the connection, as it goes on after a lost connection, once the reconnect has reopened it (#210). Such a frame is a fatal `-ERR`, such as `Stale Connection`, which the server sends to a client that left its `PING`s unanswered for a few intervals (a handler that blocks the event loop with synchronous work, a paused process), or `User Authentication Expired` for a user JWT that `jwtProvider` renews on the reconnect, or a server `PING` whose `PONG` the socket would not take. With reconnect on and `waitForReconnect` enabled, the defaults, the frame's error goes to the `errorListener` and to the logger (`NATS connection error: Server sent error frame: 'Stale Connection'`) instead of out of `handle()`; what the pulls held when the connection ended goes to the handler as part of the run, in the order it arrived, and counts in the result of `handle()` (a `stop()` or a `disconnect()` ends that delivery as above, while the client's `drain()` lets it finish, see below, a pinned group takes its pin from it as from any delivery, and a handler that throws there ends the run with its own exception); and the run then pulls on the new connection. It used to end with the frame's error although the connection was open again, so that a worker stopped consuming until something started it again. A finite run still ends with the frame's error, as above, and so does an infinite one with reconnect off or `waitForReconnect: false`, whose connection you are closing, or whose reconnect gave up, or whose new server refused the credentials, while the read that met the frame was still waiting for it. A reconnect that gives up only after that read stopped waiting for it (the read waits at most the run's `setExpiresMs()` plus one second), as one against a server that stays down does with the default options, ends the run as after a lost connection: the frame's error has gone to the `errorListener`, and `handle()` throws `Reconnect attempts exhausted`. Likewise, with `reconnectBufferSize: 0`, a pull the run issues while a reconnect that outlasted that read's wait is still under way is refused, and `handle()` throws `Connection is not open`, as after a lost connection.

An infinite run hands your handler every message it receives, across a reconnect too (#187). When the connection reconnects, the run cannot tell which of its pull requests the server still holds: a server that restarted forgot them, but one that outlived the connection (a dropped socket, a `Stale Connection` close) serves them once the reconnect has subscribed the run's reply inbox again, and a pull the run writes during the outage goes out from the reconnect buffer afterwards. So the run ends its pulls in flight and pulls again at once, and what they had received goes to your handler first, in the order it arrived; a status one of them got on the old connection, such as a `409 Consumer Deleted` or a `423` for a stale pin, neither ends the run nor drops a group's pin. What the server sends for a request the run no longer counts goes to the run's newer pulls, and what none of them has room for is held for your handler, in arrival order, behind what the pulls hold, and counts in the result of `handle()`: across a reconnect your handler can get more messages than `setBatching()` times `setDepth()`, and the run issues no new pull until it has handed them over. The iterator's `drain()`, the client's `drain()` within its budget and a run that fails hand those messages over as they hand over what the pulls hold, and `stop()`, `disconnect()` and a `drain()` whose budget has run out leave them unacked. A terminal status, such as `409 Consumer Deleted`, also hands over what the pulls behind the one it ended hold before `handle()` resolves. The run used to drop such messages, and the server delivered them again only after `ack_wait`, or never on a consumer with `ack_policy: none` or `max_deliver: 1`. A finite run (`setIterations()`) is unchanged: it does not end its pull at a reconnect, keeps its exact count, and drops a message past its last pull's batch, as `fetchBatch()` drops one past its batch. _Verified by: [PullConsumerReconnectTest](tests/Unit/PullConsumerReconnectTest.php); [PullConsumerOverflowTest](tests/Unit/PullConsumerOverflowTest.php); live: [JetStreamIntegrationTest::testAPullConsumerHandlesWhatARequestTheServerKeptAcrossAReconnectBrings](tests/Integration/JetStreamIntegrationTest.php)._

Closing the connection while a run goes on follows the rule of the connection's own queues: the client's `drain()` hands over what the run's pulls hold, and `disconnect()` discards it, wherever the run is (#207). Once the drain's flush is done, the run hands every pull in flight to your handler, in the order the messages arrived, while the connection is Draining, so that the acks your handler publishes (`ack()`, `nak()`, `term()`, `inProgress()`) still go out, and `drain()` waits for that, your handler included, within its budget (about `requestTimeoutMs`); the run then ends, and `handle()` resolves with its count, as after the iterator's `drain()`. A drain that first waits for a reconnect also hands over what the pulls held before the connection dropped, since an ack is valid on any connection. The requests your handler makes during that hand-over are taken too (#213): `ackSync()` is confirmed, and a JetStream publish or a Key/Value read or write gets its reply, also when the worker has made no request before, all within the drain's budget; one still waiting when the budget runs out fails with `Connection is not open`, and the rest of the pull is discarded and counted (see [Graceful Drain](#graceful-drain)). Before 2.24.4 those requests failed at once with `Connection is not open`, and every message the drain handed over to a handler that acked with `ackSync()` came again after the ack wait. Code the run calls (its handler, its `onError`, the `errorListener` or logger while the run reports to them) that awaits the client's `drain()` holds that drain for its whole budget, as a subscription handler that awaits it does: the drain waits for the very hand-over that waits for it, and the rest of the pull is then discarded. Stop or drain the iterator there and drain the client once `handle()` has resolved, or start the client's `drain()` from another fiber, such as a signal handler, or without awaiting it. While the client drains, the run issues no new pull, which could bring nothing once the drain has unsubscribed the run's inbox, and a run with nothing in flight ends with its count. When the budget runs out first, `drain()` closes the connection anyway: the rest is discarded from its `drain deadline exceeded` report on, which counts it, also while the connection is being closed, and `handle()` still resolves with what was handed over, unless the run was writing a pull as the drain closed the connection, which fails `handle()` with that write's error. A handler that throws during that hand-over ends the run with its own exception, as anywhere else, and a `stop()` leaves the rest undelivered; `drain()` does not wait out its budget for either. When the connection is lost, or a fatal `-ERR` ends it, during the drain's flush, the run still hands over what it holds, as the drain delivers its own backlog then, but the acks of that hand-over fail or are lost with the connection. A `disconnect()`, from your handler or from another fiber, ends whatever delivery is under way before the next message, the hand-over of a pull the run retires included, and the hand-over the client's `drain()` asked for: the rest stays unacked, for the server to deliver again after the ack wait (lost on a consumer without acks or with `max_deliver: 1`). `handle()` then fails, with `Connection is not open` where the run's next read or pull meets the closed connection, or with the transport's own error (`The connection is gone`) where its next pull's write meets a `disconnect()` from another fiber that is still closing the connection. It resolves with its count instead where the client's `drain()` had already asked the run for its hand-over, and where a finite run has no pull left to issue. A `drain()` that finds no connection to drain (its budget ran out before the reconnect it waited for was done, or that reconnect gave up) asks no run: the run's read fails, and what it holds is discarded, `handle()` failing with `Connection is not open` or the reconnect's own error (`Reconnect attempts exhausted`). Nor does a `drain()` whose flush a `disconnect()` cut short: the run fails as after any `disconnect()`. The client's `drain()` used to close the connection under the run, so that a worker that drained its client on `SIGTERM` lost what the pulls held (gone on a consumer without acks, delivered again after the ack wait otherwise), and a pull the run retired was handed over whole after a `disconnect()`, your handler running after the close had returned with every ack failing. _Verified by: [PullConsumerClientDrainTest](tests/Unit/PullConsumerClientDrainTest.php); [PullConsumerConnectionLossTest](tests/Unit/PullConsumerConnectionLossTest.php) (`testAnApplicationCloseDiscardsWhatThePullHoldsOnADisconnectAndHandsItOverOnADrain`, `testADisconnectFromTheHandlerEndsTheHandOverOfARetiredPull`, `testADisconnectFromAnotherFiberDuringTheRetirePhaseLeavesTheRestUndelivered`, `testADisconnectThatDiscardsAFullPullDoesNotEndAFiniteRunAsAnEmptyRetire`, `testADisconnectDuringTheRetirePhaseDoesNotTurnALaterPullsStatusIntoTheRunsEnd`); [PullConsumerConnectionEndingFrameTest](tests/Unit/PullConsumerConnectionEndingFrameTest.php) (`testAConnectionDrainUnderWayWhenTheFramesErrorReachesTheEngineEndsTheRunAfterTheHandOver`, `testAClientDrainDuringTheHandOverLetsItFinishAndTheRunWritesNoPullAfter`)._

The run's reply inbox (`_INBOX.JS.PULL.<nuid>.*`) is confirmed by a `PING` behind its `SUB`, as a pull fetch's is (see [JetStream Pull Consumer (Fetch + ACK)](#jetstream-pull-consumer-fetch--ack)). At the connection's subscription limit a limit `-ERR` read before that confirmation, whichever fiber's read meets it, fails `handle()` fast with a `JetStreamException` (`Pull consumer reply inbox "_INBOX.JS.PULL.<nuid>.*" may have been rejected by the server because the connection is at its subscription limit ('maximum subscriptions exceeded'). ...`), the inbox released, where the engine used to retire pull after pull at their deadlines (#175); a permissions violation naming the inbox fails it the same way (#167), also one that comes mid-run, as nats-server sends when a configuration reload withdraws the permission. Either way the handler first gets what the pulls received before the rejection, as above (#197), on a connection that is still open. A mid-run rejection that another read meets while the engine is not reading, such as your handler's own `processIncoming()`, is seen only once the run's next pulls, which no reply can reach, have waited out their deadline: up to `setExpiresMs()` plus a second later, and a finite run with no pull left to issue returns as usual, without that error. Start the consumer again once a subscription slot is free.

_Verified by: [JetStreamInboxRejectionTest](tests/Unit/JetStreamInboxRejectionTest.php) (`testAPullConsumerWhoseInboxSubIsRejectedWhileAnotherReadBringsTheErrFailsFast`, `testAPullConsumerWhoseReplayedInboxIsRejectedByTheNewServerFailsWithTheLimitError`); [PullPipelineTest::testPermissionRejectedPullInboxFailsFastInsteadOfSpinning](tests/Unit/PullPipelineTest.php); [PullConsumerConnectionLossTest::testARunWhoseInboxIsRejectedHandsTheHandlerWhatItsPullReceivedFirst](tests/Unit/PullConsumerConnectionLossTest.php)._

### Pull Consumer Priority Groups

> 📄 **Runnable example:** [`examples/pull-consumer-priority-groups.php`](examples/pull-consumer-priority-groups.php)

_Verified by: [PullConsumerIteratorTest::testHandleRePinsOnStalePin](tests/Unit/PullConsumerIteratorTest.php); [PullConsumerIteratorTest::testBuildPullIncludesAllOptionalFields](tests/Unit/PullConsumerIteratorTest.php); [JetStreamContextTest::testCreateConsumerWithPriorityGroups](tests/Unit/JetStreamContextTest.php); [JetStreamContextTest::testUnpinConsumer](tests/Unit/JetStreamContextTest.php); [JetStreamContextTest::testPinIdOf](tests/Unit/JetStreamContextTest.php)._

ADR-42 priority groups let several pull clients share one consumer while the server steers delivery. Create the consumer with `priority_groups` (a non-empty array of 1..16-character group names) and a `priority_policy` - one of `overflow`, `pinned_client`, or `prioritized`. The priority-group fields require **NATS server 2.11+**; the `prioritized` policy requires **2.12+** (an older server rejects them). See [NATS Server Version Requirements](#nats-server-version-requirements).

Then pull under a group with the `PullConsumerIterator` setters: `setGroup()` (required for any priority policy), and as the policy needs them `setPriority()` (0-9, for `prioritized`), `setMinPending()` / `setMinAckPending()` (for `overflow`), plus the general `setMaxBytes()` and `setNoWait()`. Under the `pinned_client` policy the server pins one client at a time: the iterator automatically captures the `Nats-Pin-Id` from the first delivered message and resends it on subsequent pulls, and transparently re-pins if the pin goes stale (the server returns a 423 status). Call `JetStreamContext::pinIdOf($msg)` to read a message's pin id, and `JetStreamContext::unpinConsumer($stream, $consumer, $group)` to release the active pin so another client can take over.

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\JetStream\JetStreamContext;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$js = $client->jetStream();

// Create a pull consumer with a pinned-client priority group (NATS 2.11+).
$js->createConsumer('ORDERS', 'PROC', null, [
	'priority_groups' => ['g1'],
	'priority_policy' => 'pinned_client',
])->await();

// Pull under the group. The iterator captures and resends the Nats-Pin-Id
// automatically, and re-pins transparently if the pin goes stale (423).
$totalProcessed = $js->pullConsumer('ORDERS', 'PROC')
	->setGroup('g1')
	->setBatching(10)
	->setExpiresMs(5000)
	->setIterations(5)
	->setMaxBytes(1048576)
	->handle(function (NatsMessage $msg, JetStreamContext $js): void {
		// Inspect the pin id carried by the first message of the pinned group.
		$pinId = $js->pinIdOf($msg); // string|null
		echo 'Processing: ' . $msg->payload . PHP_EOL;
		$js->ack($msg)->await();
	})->await();

echo "Processed {$totalProcessed} messages total." . PHP_EOL;

// Release the active pin so another client can take over the group.
$js->unpinConsumer('ORDERS', 'PROC', 'g1')->await();

$client->disconnect()->await();
```

### Stream Mirroring and Sourcing

> 📄 **Runnable example:** [`examples/stream-mirroring-and-sourcing.php`](examples/stream-mirroring-and-sourcing.php)

_Verified by: [StreamSourceTest](tests/Unit/StreamSourceTest.php); [features/jetstream-core/config_helpers.feature](features/jetstream-core/config_helpers.feature) (live source filtering + mirror replication)._

Use `StreamSource` to build mirror/source configuration arrays:

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\JetStream\Configuration\StreamSource;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$mirror = StreamSource::mirror('ORDERS')->toArray();

$aggregateSources = [
	StreamSource::source('ORDERS')->filterSubject('orders.>')->toArray(),
	StreamSource::source('PAYMENTS')->startSeq(100)->toArray(),
];

$remoteMirror = StreamSource::mirror('ORIGIN')
	->external('$JS.hub.API', '_DELIVER.hub')
	->toArray();

var_dump($mirror, $aggregateSources, $remoteMirror);

$client->disconnect()->await();
```

Use those arrays in `createStream()` or `updateStream()` options. Source configurations work with the current high-level API and are covered against the live fixture stack. Mirror-only stream configs also work through `createStream()` when you pass an empty `subjects` list together with the `mirror` configuration.

### Republish and Subject Transform

> 📄 **Runnable example:** [`examples/republish-and-subject-transform.php`](examples/republish-and-subject-transform.php)

_Verified by: [RepublishAndTransformTest](tests/Unit/RepublishAndTransformTest.php); [features/jetstream-core/config_helpers.feature](features/jetstream-core/config_helpers.feature) (live republish + subject transform)._

Configure republish rules and subject transforms on streams:

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\JetStream\Configuration\Republish;
use IDCT\NATS\JetStream\Configuration\SubjectTransform;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$js = $client->jetStream();

// Republish all order messages to a monitoring subject.
$js->createStream('ORDERS', ['orders.>'], [
	'republish' => Republish::create('orders.>', 'monitor.orders.>')->toArray(),
])->await();

// Republish headers only (strip payload) for lightweight notifications.
$js->createStream('EVENTS', ['events.>'], [
	'republish' => Republish::create('events.>', 'notify.events.>')->headersOnly()->toArray(),
])->await();

// Apply a subject transform to remap subjects on ingest.
$js->createStream('MAPPED', ['raw.>'], [
	'subject_transform' => SubjectTransform::create('raw.>', 'processed.>')->toArray(),
])->await();

$client->disconnect()->await();
```

## Compatibility Mapping

This repository tracks parity against the basis-company `nats.php` README examples while exposing an Amp-first API.

| Section | Status | Notes |
| --- | --- | --- |
| Connecting and Auth | workflow parity | Basic, token, username/password, JWT nonce signing, credentials file, and TLS CA/cert/key options are supported. |
| Publish Subscribe | workflow parity | Callback, queue-group, and polling queue (`SubscriptionQueue` with `fetch()`/`next()`/`fetchAll()`) patterns are supported. |
| Request Response | workflow parity | Awaited request/reply with timeout and cancellation is covered, but the API shape differs from basis-company's `dispatch()` and callback request helpers. |
| JetStream API Usage | workflow parity | Stream/consumer lifecycle, pull/push flows, ephemeral consumers, scheduling, ordered-consumer helpers, batching/iteration chain API, republish/subject-transform live behavior, mirror/source live behavior, and typed enums are covered. |
| Microservices | workflow parity | Service registration, discovery (PING/INFO/STATS/SCHEMA), grouped hierarchy, enriched endpoint stats (requests/errors/last-error/processing time), reset API, opt-in schema validation hook with built-in adapter, handler adapters (callable/object/class-string), request lifecycle observers, standardized error envelopes, and run-loop helper are covered. |
| Key Value Storage | workflow parity | Core KV flows plus update/purge/getAll/status parity are covered. |
| Object Store | parity | Uses the official on-wire layout (meta subjects keyed by `base64url(name)`, chunks under a per-object NUID subject, `SHA-256=<base64url>` digest, rollup meta), so buckets interoperate with the `nats` CLI and other clients. Bucket/object lifecycle, streaming download, object listing, chunked uploads, and digest verification are covered. Overwrites and deletes purge the previous revision's chunks. |

## Behavior Notes

### `processIncoming()`

_Verified by: [NatsConnectionTest](tests/Unit/NatsConnectionTest.php) (`testProcessIncomingDispatchesMsgToSubscriber`, `testProcessIncomingUpdatesServerInfoFromAsyncInfoFrame`); [ConcurrentReadTest](tests/Unit/ConcurrentReadTest.php)._

`processIncoming()` reads a single transport chunk, parses all complete frames from it, and dispatches them to subscription callbacks. Before it reads, it continues the subscriptions whose delivery an earlier read stopped at a handler that threw, with nothing new on the wire, and throws the next such failure without reading (see [Handler Failures](#handler-failures)). The read is bounded only by the optional `Cancellation` you pass: without one it suspends until a chunk arrives (or the peer closes the connection), so it is **not** a poll. Because one read returns only a single chunk (and TCP may coalesce several protocol messages into one chunk), call it in a loop, and pass a `TimeoutCancellation` when you need the loop to stay responsive on a quiet connection:

```php
use Amp\CancelledException;
use Amp\TimeoutCancellation;

// Process whatever arrives for up to 1 second, without parking on an idle socket.
$deadlineSeconds = hrtime(true) / 1e9 + 1.0;
while (hrtime(true) / 1e9 < $deadlineSeconds) {
	try {
		$client->processIncoming(new TimeoutCancellation(0.25))->await();
	} catch (CancelledException) {
		// No frame arrived in this slice; keep polling until the deadline.
	}
}
```

The connection reads its socket from one fiber at a time. When another fiber is already reading - a `request()` waiting for its reply, a `flush()`, the heartbeat reading the answer to its `PING` - `processIncoming()` and `readIncoming()` wait for that read to finish, bounded by the `Cancellation` you pass, and then return without reading (`0` frames): that read delivered what it read. Before they wait, they still continue the subscriptions a handler failure stopped (see [Handler Failures](#handler-failures)); what they deliver there is not counted. A loop of these calls therefore always lets the event loop run its timers and socket reads, the other fiber's read included. A flush - `flush()`, `rtt()`, `drain()`, `drainSubscription()` - ends as soon as the server answers its `PING`, whichever fiber's read takes the answer, also when that read's dispatch waited before it reached the answer; a flush whose `PING` went out before the connection dropped fails as soon as the reconnect begins, since its answer died with the old socket.

The library's operations that read while they wait for a result of their own - `request()` and `requestMany()`, a `SubscriptionQueue` poll, `fetchBatch()` and `fetchNext()`, `directGetBatch()`, a pull consumer, Key/Value `keys()` and `history()` - end their read, without reading, as soon as their reply comes or anything is delivered to their subscription by whichever fiber's read, and look again: a delivery still under way when their read started (held up in another subscription's handler that awaits, or in the write of a `PONG`), one made in the event-loop hops before their read began, or the one a reconnect they waited for makes. Such an operation also takes what is already queued for its own subscription before it reads or waits for another fiber's read, so it gets its result even while a handler ahead of it - in another fiber, or the very handler the operation runs in when a handler polls a `SubscriptionQueue` of its own - is still running (#179); the reorder is only ever of the operation's own subscription ahead of another subscription's message queued with it, and order within a subscription and between the other subscriptions is unchanged. The take is only of what is already queued when the operation looks: an operation whose own read brings the chunk runs any handler ahead of its message in that read's pass and returns once that handler does, and one started while a reconnect is under way waits for the reconnect first, within its own timeout, then takes a message already queued for it once the connection is back. `request()` and `requestMany()`, whose replies share one inbox, read with no own subscription and are unchanged. Your own reads have no such wake-up: `processIncoming()` and `readIncoming()` read until the server sends something or their cancellation fires. A custom transport's `readLine()` must keep every byte it has not returned when its cancellation ends it, a partial frame included, since the connection cancels reads to wake operations and then reads on; the built-in transports do. _Verified by: [OperationReadWakeupTest](tests/Unit/OperationReadWakeupTest.php), [ReadWakeupTest](tests/Unit/ReadWakeupTest.php), [ReadWakeupLifecycleTest](tests/Unit/ReadWakeupLifecycleTest.php)._

The client also applies asynchronous `INFO` updates received after connect, so `serverInfo()` can change during the lifetime of an open connection when the server advertises updated capabilities such as `max_payload` or cluster topology details. An `INFO` whose payload is not a JSON object, broken JSON included, is reported to the `errorListener` and the last server info is kept; the `INFO` the server greets a new connection with fails the connect, or the reconnect attempt, instead. _Verified by: [NatsConnectionTest::testMalformedAsyncInfoDoesNotTearDownTheReadLoop](tests/Unit/NatsConnectionTest.php), [ServerErrorKeepingConnectionTest](tests/Unit/ServerErrorKeepingConnectionTest.php) (`testAnInfoThatIsNotAJsonObjectIsReportedWithoutFailingTheRead`, `testAnInitialInfoThatIsNotAJsonObjectFailsTheConnectAsBrokenJsonDoes`)._

### Heartbeat and Request Timeouts

_Verified by: [NatsConnectionTest](tests/Unit/NatsConnectionTest.php) (`testIdleConnectionStaysOpenViaHeartbeatSelfRead`, `testHeartbeatReadHandlesEmptyErrorAndFatalFrames`, `testRequestTimeoutCancelsReadAndAllowsSubsequentRequest`, `testMaxPingsOutTriggersReconnect`); live: [NatsClientIntegrationTest](tests/Integration/NatsClientIntegrationTest.php) (`testIdleConnectionStaysOpenViaHeartbeat`, `testRequestTimeoutDoesNotPoisonConnection`)._

The heartbeat timer answers its own `PONG`: after sending a `PING` it performs a short, bounded read to consume the reply (unless an application `processIncoming()` read is already in flight). Liveness detection therefore does not depend on the application continuously calling `processIncoming()`, so an otherwise idle connection (for example a pure publisher) is not closed by spurious `maxPingsOut` detection. Only an actual server `PONG` resets the outstanding-ping counter - other inbound frames (data, `INFO`, `PING`) do not - so a server that keeps sending data but stops answering `PING`s is still detected via `maxPingsOut`.

Request and pull-fetch timeouts cancel the underlying socket read rather than leaving it pending. A timed-out `request()` (or `fetchBatch()`/`fetchNext()`) cleanly releases the read, so it cannot orphan an in-flight read or trigger a spurious reconnect on the next operation.

### Reconnect Behavior

_Verified by: [NatsConnectionTest](tests/Unit/NatsConnectionTest.php) (`testBackoffDelayIsExponential`, `testConnectRotatesServersOnReconnectAttempts`, `testReconnectRetriesWhenResubscribeGetsFatalServerError`, `testReconnectBackoffDelayProgression`, `testReconnectAttemptsExhaustedReturnsClosed`); live: [NatsClientIntegrationTest](tests/Integration/NatsClientIntegrationTest.php) (`testSeveredLiveConnectionMidIdleReconnectsAndResumesDelivery`, `testConnectWithServerRotationFallback`)._

When a connection drops and `reconnectEnabled` is `true`:

1. **Exponential backoff**: delay is computed as `reconnectDelayMs * 2^(attempt - 1)`, capped at `reconnectMaxDelayMs`, with random jitter up to `reconnectJitterMs`.
2. **Server rotation**: the client cycles through configured servers in order.
3. **Subscription replay**: all active subscriptions are replayed (SUB commands resent) after reconnect. A subscription removed while the reconnect is between replaying and going live - `unsubscribe()` can then only remove it locally - is unsubscribed on the new connection before it goes live, so the server does not keep delivering to it; an auto-unsubscribe max armed in that window (`unsubscribe($sid, $max)`) is sent there too, counting only what was received before the outage. A `drainSubscription()` issued in that window waits for the reconnect and drains on the new connection, so the messages the server still sends on the re-subscribed sid are delivered. _Verified by: [DrainLifecycleTest](tests/Unit/DrainLifecycleTest.php) (`testSubscriptionRemovedWhileAReconnectReplaysIsUnsubscribedBeforeTheConnectionOpens`, `testAutoUnsubscribeArmedWhileAReconnectReplaysReachesTheServer`, `testAutoUnsubscribeArmedWhileAReconnectReplaysCountsWhatWasReceivedBefore`, `testDrainSubscriptionWhileAReconnectReplaysDeliversWhatTheServerSendsBeforeItsUnsubscribe`)._
4. **Replay validation**: reconnect does not treat replayed subscriptions as successful if the server immediately answers with a fatal `-ERR` during replay. In that case reconnect keeps retrying until a healthy server accepts the replay or attempts are exhausted. A rejection the server keeps the connection open for, such as a replayed SUB beyond the maximum subscriptions, is reported to the `errorListener` instead, and the reconnect completes (see the `-ERR` kinds below).
5. **Published messages during reconnect are buffered and replayed**: while a reconnect is in flight, publishes are held in an outbound buffer (up to `reconnectBufferSize`, default 8 MiB, matching nats.go) and flushed in order on a successful reconnect. A plain `publish()` or `publishWithHeaders()` issued on an Open connection writes directly; if that write discovers a dead socket, it runs one recovery inline and retries afterward, including with `waitForReconnect: false`. These APIs have no timeout or caller cancellation. Requests use the bounded send path described in item 6. A publish is rejected (throws) only when the buffer is full, when `reconnectBufferSize` is `0` (buffering disabled), when the connection is closed / not reconnecting, or while `disconnect()` or `drain()` is closing it (the reconnect is stopping, so nothing would ever send the buffered bytes). _Verified by: [NatsConnectionTest](tests/Unit/NatsConnectionTest.php) (`testPublishBuffersDuringReconnectAndFlushesOnReconnect`, `testPublishWithHeadersBuffersDuringReconnectAndRecordsOutbound`, `testPublishBufferOverflowThrowsDuringReconnect`)._
6. **Operations issued during a reconnect wait for it**: `request()`, `requestWithHeaders()`, `requestMany()`, `subscribe()`, `flush()`, `rtt()`, `processIncoming()`/`readIncoming()`, `SubscriptionQueue` polling, `drain()` and `drainSubscription()` wait for an in-flight reconnect within their own timeout instead of failing at once with `Connection is not open`, then run on the new connection. The timeout is one budget: a request's covers the wait, the set-up of the reply inbox, the publish and the reply; `subscribe()`, `flush()`, `rtt()`, `drain()` and `drainSubscription()` use `requestTimeoutMs`; `processIncoming()` is bounded by the cancellation you pass, and without one waits for the whole reconnect; a `SubscriptionQueue` waits within its timeout, so `fetch()`, and `next()` or `fetchAll()` without a timeout, return nothing during a reconnect instead of throwing. An operation whose wait runs out throws a `TimeoutException`. A `drain()` whose budget runs out still closes the connection, and a `drain()` that cannot wait closes it and throws - see [Graceful Drain](#graceful-drain). A reconnect that gives up surfaces its own error (for example `Reconnect attempts exhausted`), though not to the operation whose read brought a lame-duck `INFO` (item 9): that failover reports its failure through the `errorListener` and a `Closed` event, and the operation finds the connection closed when it next looks, failing with `Connection is not open`. A publish buffered during a reconnect yields one event-loop tick. One of the library's operations whose read waited for the reconnect looks again before it reads the new socket, so a pull consumer ends its pulls in flight and pulls again right after the reconnect, not at their deadline, and what those pulls received, or what the server still sends for them, reaches its handler (#187, see [Pull Consumer Batching/Iteration](#pull-consumer-batchingiteration)). The waiting is what lets a synchronous application - a queue worker or daemon that only ever awaits one operation at a time - drive a reconnect the heartbeat started in the background: a reconnect advances only while something waits on the event loop, so operations that failed on the spot used to leave it stalled until the process restarted. Operations still fail at once when no reconnect is in flight (a closed connection) and when called from a connection/error listener while the reconnect is still in flight - a `Disconnected` listener, say - since the reconnect waits for that listener. A `Reconnected` listener runs once the reconnect is over, and its operations run as usual: one that finds the new connection gone already reconnects it again (see item 8). Your own `processIncoming()` or `readIncoming()` whose read is the first to notice a dead connection runs the reconnect itself and waits for all of it. One of the library's operations whose own read is the first to notice - a `request()` or `requestMany()`, a `SubscriptionQueue` poll, `fetchBatch()`, `fetchNext()` or `directGetBatch()`, a pull consumer, Key/Value `keys()` or `history()`: every operation that reads for a result of its own - or whose read brings a lame-duck `INFO` (item 9), starts the reconnect and waits for it only within its own timeout, like a failed write does: it times out while the reconnect carries on, or returns the result that a delivery still under way brings during the outage, and with `waitForReconnect: false` it fails at once with `Connection is not open`, the read's error as its previous (none for a lame-duck `INFO`). A `requestMany()` or `fetchBatch()` (and so `fetchNext()`) whose read fails that way, or because the reconnect gave up or reconnect is off, or because it brought a fatal `-ERR` (see the `-ERR` kinds below), returns the replies or the partial batch it has received, and throws the read's error only when it has received nothing. A pull consumer run whose read fails that way, or because the reconnect gave up or reconnect is off, or whose pull's write fails because the reconnect it ran gave up or reconnect is off, hands its handler what its pulls have received, in order, and `handle()` then throws that error (#197, see [Pull Consumer Batching/Iteration](#pull-consumer-batchingiteration)), and so does a finite run whose read meets a fatal `-ERR` or a `PONG` the socket would not take. An infinite run whose read meets one with waiting enabled goes on instead, as it goes on after a lost connection: the error goes to the `errorListener` and the logger, the handler gets what the pulls held as part of the run, and the run pulls again on the new connection once the reconnect is done; a reconnect that gives up ends the run, with the frame's error when it gave up while that read was still waiting for it, and with `Reconnect attempts exhausted` when it gave up later, as after a lost connection (#210). A `flush()`, `rtt()` or `drainSubscription()` flush whose own read is the first to notice fails with `Connection lost before the server answered the PING` as soon as the reconnect's first attempt ends the dead connection's `PONG`s, as one waiting for a reconnect another fiber runs does, or at once with `Connection is not open` when waiting is disabled; `drainSubscription()` reports that and removes the subscription while the reconnect runs on. An operation that times out before such a reconnect gives up does not see `Reconnect attempts exhausted`: the `Closed` event carries it. A `request()`, `requestWithHeaders()` or `requestMany()` whose own PUB/HPUB write is the first to notice also waits within its original budget, or fails at once with `Connection is not open` when waiting is disabled. It retries at most once after recovery, rechecking the deadline and shared reply inbox before writing; it never enters the reconnect publish buffer and is never retried after timeout or cancellation. Inline socket write backpressure is included in this budget. A transport write already started can finish after the caller times out or cancels, so either outcome can still have server-side effects. `requestMany()` throws `TimeoutException` if the send cannot complete within its budget; after a successful send, its collection timeout returns the replies collected, possibly none. A `subscribe()`, `flush()` or `rtt()` whose own write is the first to notice starts the reconnect and waits for it the same way, within its timeout, and a `subscribe()` then runs on the new connection; so does the request whose write of the reply inbox's SUB is the first to notice, within the request's own timeout and cancellation (#194), never sent once that budget has run out; a `flush()` or `rtt()` fails anyway, with `Connection lost before the server answered the PING`, since what it was to confirm went to the dead connection. `unsubscribe()` does not throw on a dead socket - the server dropped the subscription with the connection - and leaves the reconnect to the next operation that needs the socket. Set `waitForReconnect: false` to fail fast instead. _Verified by: [WaitForReconnectTest](tests/Unit/WaitForReconnectTest.php), [FailedControlWriteTest](tests/Unit/FailedControlWriteTest.php), [FailedRequestWriteTest](tests/Unit/FailedRequestWriteTest.php), [OperationReadReconnectTest](tests/Unit/OperationReadReconnectTest.php), [OperationReadReconnectLifecycleTest](tests/Unit/OperationReadReconnectLifecycleTest.php), [OperationReadLameDuckFailoverTest](tests/Unit/OperationReadLameDuckFailoverTest.php), [PullConsumerConnectionLossTest](tests/Unit/PullConsumerConnectionLossTest.php), [PullConsumerConnectionEndingFrameTest](tests/Unit/PullConsumerConnectionEndingFrameTest.php); live: [NatsClientIntegrationTest](tests/Integration/NatsClientIntegrationTest.php) (`testRequestIssuedDuringReconnectWaitsForItAgainstALiveServer`, `testSubscribeIssuedDuringReconnectDeliversAfterItAgainstALiveServer`, `testRequestDuringReconnectFailsFastAgainstALiveServerWhenWaitingIsDisabled`, `testDrainDuringReconnectDeliversBufferedPublishesAgainstALiveServer`), [JetStreamIntegrationTest](tests/Integration/JetStreamIntegrationTest.php) (`testJetStreamPublishDuringReconnectWaitsForTheAckAgainstALiveServer`, `testJetStreamFetchDuringReconnectWaitsForItAgainstALiveServer`); [features/resilience/client_resilience.feature](features/resilience/client_resilience.feature)._ `fetchBatch()` / `fetchNext()` share their expiry plus a second across setup, recovery and collection, while batched Direct Get uses that interval as a renewable no-progress budget; these batch setup waits are not capped by `requestTimeoutMs`. Their own failed PUB writes obey `waitForReconnect` too (#188).
7. **Closing stops a reconnect promptly**: `disconnect()` and `drain()` stop an in-flight reconnect at once. They cut its backoff delay short and stop its dial - Amp's retry pauses between refused attempts included - and a step of its handshake fails as soon as they close the socket. Both then wait for that reconnect, or a `connect()` still dialling, to end before they return, so a `connect()` issued after the close dials afresh. A `Reconnected` listener is not part of the reconnect: it runs once the reconnect is over, and they do not wait for one still running. A custom transport that cannot stop its dial - one that does not implement `CancellableDialTransportInterface` - is waited for, up to `connectTimeoutMs`. A `connect()` issued while the close is still winding the reconnect down fails at once, because the close wins, and a close called from a connection/error listener inside the reconnect does not wait for it. A reconnect stopped this way reports no exhaustion - the close is the caller's - and delivers nothing: what is queued is the close's to deliver (`drain()`) or discard (`disconnect()`). A `disconnect()` also wins over a `connect()` that is still dialling or retrying its first dial, the last retry included. And a read, write or heartbeat that fails on a connection you have since closed and reopened leaves the new connection alone. A reconnect the server refuses to authenticate reports the buffered publishes it discards, as one that runs out of attempts does. Every close is announced with one `Closed` event, also when a `disconnect()` - or a `drain()` running out of time - comes while a connect or reconnect that gave up is still closing the socket: that path announces it. _Verified by: [DrainLifecycleTest](tests/Unit/DrainLifecycleTest.php) (`testConnectRightAfterADrainThatRanOutOfTimeSucceeds`, `testDisconnectDuringTheLastReconnectBackoffDoesNotReportExhaustion`, `testDrainWaitingForAReconnectThatFailsAuthenticationEndsClosedAndReportsTheDiscardedPublishes`), [CloseIntentTest](tests/Unit/CloseIntentTest.php), [AmpSocketTransportTest](tests/Unit/AmpSocketTransportTest.php) and [WebSocketTransportTest](tests/Unit/WebSocketTransportTest.php) (`testDialStoppedDuringTheRetryPause...`)._
8. **Connection listeners hear of a reconnect once it is over**: a reconnect announces the new connection - `Reconnected`, or `Connected` when it completed a failed initial `connect()` - once it is over, after letting go of the operations that waited for it. Those operations, a `connect()` that joined the reconnect and publishes parked on its flush may then run while the listener is still busy; an operation whose own read or write was the first to notice the dead connection, or whose read brings a lame-duck `INFO` (item 9), and which so started the reconnect, returns after the listener when it waits for the reconnect to its end - your own `processIncoming()` always does, the library's operations only while their timeout lasts and nothing else ends their wait (see item 6) - unless the announcement is delivered from the event loop, see below. A `drain()` that waited for the reconnect may therefore drain and close the connection before a `publish()` whose failed write ran the reconnect is retried - see [Graceful Drain](#graceful-drain). A close the listener makes takes over what the reconnect read, even one still under way when the listener returns: `drain()` delivers it, `disconnect()` discards it, though a read that receives anything during the close still delivers it with what it received, as any read does. What the listener calls runs as it would anywhere else: an operation that finds the new connection gone already reconnects it again, and the listener hears `Disconnected` and `Reconnected` - or one `Closed`, if that reconnect gives up - while its call is still running. An open announced while another `Connected` or `Reconnected` call is under way - typically the call whose operation had to reconnect - is delivered from the event loop rather than from that operation, so that listener calls do not nest one level deeper with every connection a flapping server drops; that operation then goes on at once, and the messages the reconnect read can reach their subscription handlers before the listener hears of the connection. The listener hears of a connection only while it is still the open one: one closed, being closed, replaced or lost before the listener could be told is not announced, and when it was lost, neither is its loss, so a `Reconnected` listener always finds the connection Open and never hears of it after the `Closed` that ended it. The logger records every transition all the same. Do not hold a lock across an await on the same connection in such a listener: a call nested in it may need that lock. When a `Connected` listener lets the new connection die and returns with the next reconnect in flight, the `connect()` that dialled fails with `Connect was aborted before the connection opened`, while a `connect()` that joined it has already succeeded; the connection comes back once that reconnect is done. _Verified by: [ReconnectedListenerTest](tests/Unit/ReconnectedListenerTest.php), [DrainLifecycleTest](tests/Unit/DrainLifecycleTest.php) (`testDrainDoesNotWaitForASlowReconnectedListener`, `testSupervisorReconnectingAfterADrainFromAReconnectedListenerOpensANewConnection`)._
9. **Lame-duck failover**: a server about to shut down says so with an async `INFO` carrying `"ldm":true` (lame duck mode), usually naming the servers still available in `connect_urls`. The connection emits `LameDuck` (after `DiscoveredServers`, when the `INFO`'s `connect_urls` differ from the last ones seen) and, when reconnect is enabled and the pool holds more than one server, fails over to another one at once instead of waiting for the server to close the connection. The failover is a reconnect like any other (`Disconnected`, then `Reconnected`, the subscriptions replayed, buffered publishes flushed), and where it runs depends on the read that brought the `INFO`. The rest of the chunk the `INFO` came in is handled for the connection it was read on: its messages are delivered to their subscriptions, a late reply on the request inbox included, which does not count as the new server's confirmation of the replayed inbox, while its `PING`, `PONG`, `INFO` and `-ERR` frames are the old server's and are not applied to the new connection - the `PING` gets no `PONG` on it, the `PONG` confirms nothing on it, the `INFO` neither replaces the new server's info nor starts a second failover, and an `-ERR` among them is reported through the `errorListener` (`The server the connection left sent an error frame: ...`) without failing the read. A `LameDuck` listener that closes the connection itself (`disconnect()`, say) leaves no failover to run, and the rest of the chunk is the closed connection's: its `PING` gets no `PONG` on the closed socket, and its messages go with the connection, as what a `disconnect()` discards does. Your own `processIncoming()` or `readIncoming()`, a serving loop's read and the heartbeat's run it inline and return once it is over, the connection on the other server or ended because no server could be reached, which the `errorListener` and a `Closed` event report. The read of one of the library's operations (a `request()` waiting for its reply, a `SubscriptionQueue` poll, a fetch, a `flush()` waiting for its `PONG`) starts it in a fiber of its own and waits for it only within the operation's timeout, as for a reconnect its read starts (item 6): the operation times out at its deadline, or returns what is delivered meanwhile, and with `waitForReconnect: false` fails at once with `Connection is not open`, unless what it waits for came in the same read (a flush's `PONG`, ahead of the `INFO` or behind it; a request's reply), a `requestMany()` or `fetchBatch()` returning what it has received instead, and a pull consumer run handing its handler what its pulls received before `handle()` throws (#197); the failover carries on and is announced once it is over, or reports its failure through the `errorListener` and a `Closed` event. Not when bytes behind the `INFO` in the same chunk fail to parse: that read runs the failover inline, as your own does, since it recovers the corrupt stream inline right after. A `PONG` behind the `INFO` still answers the leaving connection's `PING` until the failover dials. With reconnect disabled, or a pool of one, there is no failover, and the chunk is handled as any other. _Verified by: [LameDuckChunkRemainderTest](tests/Unit/LameDuckChunkRemainderTest.php), [OperationReadLameDuckFailoverTest](tests/Unit/OperationReadLameDuckFailoverTest.php), [PullConsumerConnectionLossTest](tests/Unit/PullConsumerConnectionLossTest.php) (`testARunWhoseReadFailsWithTheConnectionGoingHandsTheHandlerWhatItsPullReceivedFirst`), [ReconnectedListenerTest](tests/Unit/ReconnectedListenerTest.php) (`testLameDuckInfoReadInAReconnectedListenerFailsOver`, `testPingBehindALameDuckInfoReadInAReconnectedListenerIsNotAnsweredOnTheNewConnection`), [NatsConnectionTest](tests/Unit/NatsConnectionTest.php) (`testConnectionListenerReceivesLameDuckAndDiscoveredServers`, `testLameDuckWithFailoverEmitsErrorWhenRecoveryFails`)._

Server `-ERR` frames come in three kinds:

- **Reported.** `Invalid Subject`, `Permissions Violation for Publish to ...` and `Permissions Violation for Subscription to ...` neither fail a read nor close the connection: they are passed to the `errorListener` once the read that brought them has queued the rest of what it read. When the subscription the server refused is the shared reply inbox, request/reply fails fast until the connection closes for good - see [Request/Reply](#requestreply).
- **Failing the read, keeping the connection.** `maximum subscriptions exceeded`, `Invalid Publish Subject` and any other `Permissions Violation` (such as `... for Publish with Reply of ...`) fail the read that brought them, `flush()` and `rtt()` included, and the connection stays open. The heartbeat's read, a service's `run()`, the flushes of `drain()`, `drainSubscription()` and a service's `drain()`, and a reconnect's replay report them to the `errorListener` instead and carry on, the flushes reading on to their `PONG`. So a reconnect whose replayed SUB the server rejects completes; a rejection that arrives after the replay's short poll fails the read that brings it. When the server rejects the shared reply inbox for the subscription limit, the next request subscribes it again - see [Request/Reply](#requestreply). The JetStream pull fetch, pull consumer and batched Direct Get inboxes are confirmed the same way, by a `PING` behind their `SUB`: a limit `-ERR` read before that fails the call at once, whichever read meets it, and the inbox is unsubscribed - see [JetStream Pull Consumer (Fetch + ACK)](#jetstream-pull-consumer-fetch--ack) (#175). If the server closes the connection anyway, as it does after `maximum subscriptions exceeded` when an account's subscription limit is lowered, the EOF ends it.
- **Fatal.** Any other `-ERR` - `Stale Connection`, `Authorization Violation`, `Maximum Payload Violation`, ... - comes right before the server closes the connection. `Stale Connection`, for one, goes to a client that stopped answering its pings, which a synchronous application that sat idle for a few minutes meets on its next call. The connection then ends like one whose read failed, wherever the `-ERR` is read, the heartbeat's own read included: it reconnects, or with reconnect off closes for good, and the read that brought the `-ERR` fails with the server's error once the connection is Closed or, with reconnect on, once the reconnect is done or that read's own timeout runs out (it does not wait with `waitForReconnect: false`). A `requestMany()`, `fetchBatch()` or `fetchNext()` whose read brought it returns what it had received instead, whatever the reconnect has done by then, and fails with the server's error only when it has received nothing (#196). Before 2.24.6 it failed with the error, and what it had received was lost, whenever the reconnect had already reopened the connection, as a quick one does: replies the caller never saw, and fetched messages the server counted as delivered, which came again only after the ack wait (never on a consumer without acks or with `max_deliver: 1`). An infinite pull consumer run whose read brought it reports the error to the `errorListener` and the logger instead of throwing it and goes on, as after a lost connection, pulling again once the reconnect is done; a finite run ends with it, and so does an infinite one with reconnect off, with `waitForReconnect: false`, or whose reconnect gave up while that read was still waiting for it; a reconnect that gives up later ends the run with `Reconnect attempts exhausted`, as after a lost connection (#210, see [Pull Consumer Batching/Iteration](#pull-consumer-batchingiteration)). A fatal `-ERR` read together with one of the others still ends the connection.

A server `PING` whose `PONG` the socket would not take ends the connection like a fatal `-ERR` (during a `drain()` it ends the drain's flush instead). Nothing else a frame raises ends the connection. _Verified by: [ConnectionEndingFrameTest](tests/Unit/ConnectionEndingFrameTest.php), [PullConsumerConnectionEndingFrameTest](tests/Unit/PullConsumerConnectionEndingFrameTest.php), [ServerErrorKeepingConnectionTest](tests/Unit/ServerErrorKeepingConnectionTest.php), [DrainLifecycleTest](tests/Unit/DrainLifecycleTest.php) (`testDrainFlushStillEndsAtOnceAtAPongTheSocketWouldNotTake`, `testAServiceReadingDuringADrainReportsAPongTheSocketWouldNotTakeOnce`), [OperationReadReconnectTest](tests/Unit/OperationReadReconnectTest.php) (`testACollectionWhoseReadMeetsAFatalErrorReturnsWhatItReceivedAlsoOnceTheReconnectReopenedTheConnection`, `testACompleteCollectionWhoseChunkAlsoBringsAFatalErrorReturnsItsResult`, `testACollectionWhoseReadFailsWithTheConnectionStayingOpenStillFails`)._

The initial handshake is bounded by `connectTimeoutMs`, not by a fixed number of transport reads. During bootstrap the client will also answer server `PING` frames and process async `INFO` updates while waiting for the initial `PONG`.

### Slow Consumers

_Verified by: [SlowConsumerErrorPolicyTest](tests/Unit/SlowConsumerErrorPolicyTest.php); [ReadReportTest](tests/Unit/ReadReportTest.php); [NatsConnectionTest](tests/Unit/NatsConnectionTest.php) (`testSlowConsumerDropOldestPolicy`, `testSlowConsumerDropNewestPolicy`, `testErrorListenerReceivesSlowConsumerDrop`, `testSlowConsumerErrorPolicyThrows`, `testSlowConsumerErrorPolicyOverflowIsASlowConsumerExceptionNamingTheSid`, `testSlowConsumerErrorPolicyOverflowSurfacesToListenerOnceNotTwice`, `testSlowConsumerErrorPolicyOverflowCountsTowardAutoUnsubAndDoesNotLeak`); [SubscriptionQueueTest](tests/Unit/SubscriptionQueueTest.php) (`testEnqueueDropOldestIncrementsDroppedCountAndNotifiesErrorListener`, `testEnqueueDropNewestIncrementsDroppedCountAndNotifiesErrorListener`, `testEnqueueThrowsOnOverflowWhenPolicyIsError`)._

Each subscription buffers the messages read for it until its handler takes them, up to `maxPendingMessagesPerSubscription` (1024 by default). The inboxes the client itself uses to collect a known number of messages - JetStream fetches and pull consumers, batched Direct Get, Key/Value `keys()` and `history()` - are exempt: the request bounds what they collect. When a subscriber falls behind and its buffer is full, `slowConsumerPolicy` decides what happens to the next message:

| Policy | What is dropped | How you find out |
|---|---|---|
| `DropOldest` (default) | the oldest buffered message, to make room | a `NatsException` ("Slow consumer on sid N: dropped oldest message") passed to the `errorListener`, and logged at debug level |
| `DropNewest` | the new message | the same, with "dropped newest message" |
| `Error` | the new message | a `SlowConsumerException`, thrown or passed to the `errorListener` and logged at error level - see below |

Core NATS does not resend a dropped message; a JetStream consumer with acknowledgements redelivers it once its `ack_wait` expires. A dropped message still counts toward an auto-unsubscribe limit set with `unsubscribe($sid, $max)`, because the server counted it when it sent it.

Whatever the policy, a read reports what it dropped once it has queued everything else it read. So an `errorListener` may react by reading on the connection - `drainSubscription()` to stop a subscriber that cannot keep up, say - without messages overtaking one another. A logger that throws on a report fails neither the read nor the delivery of the messages the policy kept.

`Error` loses the message just as the drop policies do; what it adds is that the loss is hard to miss. The `SlowConsumerException` is a `ConnectionException` whose `sid` names the subscription that fell behind - the connection itself is fine. Where it surfaces depends on which read ran into the full buffer:

- **Your own reads throw it.** `processIncoming()` and `readIncoming()` deliver everything else they read, then throw the exception; a second overflow in the same read is passed to the `errorListener`. Catch it and keep reading. Any other failure in the same read, such as an `-ERR`, is thrown in its place, and the overflow is passed to the `errorListener`.
- **Operations report it and complete.** Many calls read the socket themselves while they wait for a result of their own: `request()`, `requestMany()` and everything built on them (JetStream publish, `ackSync()`, stream and consumer management, Key/Value and Object Store calls), `flush()`, `rtt()`, `fetchBatch()`/`fetchNext()`, `directGetBatch()`, pull consumers, Key/Value `keys()` and `history()`, and `SubscriptionQueue` polling. Such a read picks up messages for every subscription. When one of those messages overflows another subscription's buffer, the operation does not fail: the exception is passed to the `errorListener` and the operation returns its result. Only an overflow of the operation's own subscription - a `SubscriptionQueue` polling its own subject - fails it, once the rest of that read is delivered.
- **Background reads report it.** The heartbeat's own read, a reconnect re-subscribing, and the flushes of `drain()` and `drainSubscription()` report the overflow and carry on. So does `Service::run()`, which has no caller to fail.

Each of these overflows reaches you exactly once: thrown to the read, or passed to the `errorListener` and logged at error level. An overflow of another subscription found by an operation, and every overflow found in the background, is only reported, so with `Error` register an `errorListener` - one that drains the subscription that fell behind (`drainSubscription($error->sid)`) is a natural reaction:

```php
use IDCT\NATS\Connection\Enum\SlowConsumerPolicy;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\SlowConsumerException;

$client = new NatsClient(new NatsOptions(
    slowConsumerPolicy: SlowConsumerPolicy::Error,
    errorListener: static function (\Throwable $error): void {
        if ($error instanceof SlowConsumerException) {
            // A message for subscription $error->sid was dropped: its handler cannot keep up.
        }
    },
));
$client->connect()->await();

try {
    $client->processIncoming()->await();
} catch (SlowConsumerException $e) {
    // The same, found by your own read; everything else it read was delivered.
}
```

Previously an overflow failed whichever operation's read happened to run into it - even a `request()` whose reply had already arrived, or a fetch that had already collected its messages. To keep that behavior, set `slowConsumerErrorsFailOperations: true`: the operations listed above then fail with an overflow of any subscription, as `processIncoming()` does. `Service::run()` and the background reads still only report it. An overflow of a `SubscriptionQueue`'s polling buffer (below) that is thrown is then also passed to the `errorListener`, as it was before, so that code which swallows an operation's failure cannot lose it.

A `SubscriptionQueue` keeps its own polling buffer on top of the subscription's, with the same limit and policy, and counts what it drops in `droppedCount()`. That buffer fills when your application does not poll the queue often enough, and its overflow follows the same rules: under `Error` it is a `SlowConsumerException` naming the queue's subscription, thrown to your own read and reported when an operation or a background read runs into it. Either way the messages behind it in the same read, for other subscriptions, are still delivered. A poll that fails on its own subscription's overflow keeps what it had already taken from the buffer: `fetchAll()` puts those messages back, in order, for the next call. Messages that arrived before `subscribeQueue()` returned the queue, and do not fit in it, are counted and reported. Under `DropOldest` and `DropNewest` its drops are reported like the subscription's.

### Handler Failures

A subscription handler that throws fails the read that ran it only when that read is your own. Where the exception surfaces depends on which read ran the handler:

- **Your own reads throw it.** `processIncoming()` and `readIncoming()` throw the exception of the handler that threw, once the read has delivered the messages it brought for the other subscriptions: a `request()`, a JetStream fetch or a `SubscriptionQueue` poll in another fiber whose result is among them gets it from that read, rather than with the server's next bytes. Only the failing subscription's own delivery stops at the failing message: its later messages stay queued, in order, and your next `processIncoming()` or `readIncoming()` continues with them before it reads, with nothing new on the wire (as does any read that receives anything, or a serving loop's read); its result still counts only what it read. When that read is your own it throws again if the next handler throws, so each failure of that subscription reaches you one read at a time; an operation's or a background read that delivers them reports the failure instead. If you stop reading instead, they wait for the next read that receives anything, and a `disconnect()` discards them. The later subscriptions of a delivery held up in another fiber's handler that awaits are not continued, unless a handler failure had already stopped one of them, which is then continued whole, in order, the messages the held-up delivery brought for it included; the others wait for that delivery, which goes on once the handler returns. When a second subscription's handler throws in the same read, that exception is passed to the `errorListener` and logged at error level, since one read throws one exception; without an `errorListener` or a logger it is seen nowhere. Catch inside the handler what it can handle, and otherwise catch the exception and keep reading.
- **Operations report it and complete.** The calls that read the socket while they wait for a result of their own - the operations listed under [Slow Consumers](#slow-consumers) - deliver messages for every subscription. When another subscription's handler throws there, the exception is passed to the `errorListener` and logged at error level, the rest of the read is delivered, and the operation returns its result: a `request()` whose reply arrived behind it gets the reply, a fetch keeps its messages, and a `request()` made inside a service endpoint does not turn into a `HANDLER_ERROR` reply. That includes a `CancelledException` a handler throws, which an operation does not take for the end of its own wait. A handler of the operation's own subscription that throws still fails it; that is only ever a subscription the library makes for the operation, such as a `SubscriptionQueue`'s or a fetch's, never one of yours.
- **Background reads report it.** The heartbeat's read, a reconnect (its handshake, and the delivery after it, which an operation's own read may run), the flushes of `drain()` and `drainSubscription()`, and a service's `run()` report it and deliver the rest.

A failure that a read reports - an operation's, a background read's, or your own read's for a second subscription that fails in the same read - is visible nowhere without an `errorListener` or a logger, so register an `errorListener`, or catch inside the handler what it can handle.

Previously another subscription's handler failure failed whichever operation's read happened to run that handler - even a `request()` whose reply had already arrived. To keep that behavior, set `handlerErrorsFailOperations: true`: the operations then fail with the exception of any handler their read runs, as `processIncoming()` does, after that read has delivered the other subscriptions' messages behind it, as `processIncoming()` does too; only the failing subscription's later messages wait for a later read. The background reads still only report it. A handler's `CancelledException` is then taken for the end of the operation's own wait, as it was: the operation stops waiting early or times out, and the exception is not reported - unless the operation's own result arrived in the same read behind it, which the read delivers first: the operation then completes with it, and the `CancelledException` is neither thrown nor reported.

An `-ERR` that fails a read but leaves the connection open, such as `maximum subscriptions exceeded`, is different: it fails the operation whose read meets it, whichever subscription or publish it answers. It names neither, and it is often the answer to what the operation itself sent, its `SUB` or its `PUB`. See [Reconnect Behavior](#reconnect-behavior).

_Verified by: [HandlerFailureDuringOperationTest](tests/Unit/HandlerFailureDuringOperationTest.php), [HandlerFailureInYourOwnReadTest](tests/Unit/HandlerFailureInYourOwnReadTest.php), [ServerErrorKeepingConnectionTest](tests/Unit/ServerErrorKeepingConnectionTest.php) (`testAnOperationsReadReportsAHandlerThatThrowsUnlessConfiguredToFail`, `testYourOwnReadThrowsAfterDeliveringTheOtherSubscriptionsAndAServingReadDeliversTheRemainder`, `testYourOwnNextReadDeliversTheRemainderAndSaysItReadNothing`, `testAnOperationsReadLeavesTheRemainderToYourNextRead`), [CloseIntentTest](tests/Unit/CloseIntentTest.php) (`testYourReadDuringADisconnectLeavesTheRemainderOfAFailingSubscriptionToTheClose`, `testAPassUnderWayWhenADisconnectBeginsGoesOnToTheOtherStoppedSubscriptions`), [DrainLifecycleTest::testYourReadDuringADrainLeavesARemainderUnderWayToItsDeliveryAndReads](tests/Unit/DrainLifecycleTest.php), [WaitForReconnectTest::testYourReadContinuingARemainderLeavesTheHandshakeToTheRecovery](tests/Unit/WaitForReconnectTest.php), [MuxInboxRejectionTest::testAHandlerFailureWhileARequestWaitsForTheNewMuxIsReportedUnlessConfiguredToFail](tests/Unit/MuxInboxRejectionTest.php), [SlowConsumerErrorPolicyTest::testAnOverflowEscapingAHandlerIsThatHandlersFailure](tests/Unit/SlowConsumerErrorPolicyTest.php)._

### Ordered Consumer Gap Recovery

_Verified by: [JetStreamContextTest](tests/Unit/JetStreamContextTest.php) (`testSubscribeOrderedConsumerRecreatesOnSequenceGap`, `testSubscribeOrderedConsumerDeliversFilteredMessagesWithoutSpuriousRecreate`); [JetStreamIntegrationTest::testJetStreamOrderedConsumerWithFilteredSubjectAfterPriorMessages](tests/Integration/JetStreamIntegrationTest.php)._

`subscribeOrderedConsumer()` tracks the JetStream **consumer** delivery sequence (which is contiguous per delivery, even for a filtered consumer over a stream that also carries non-matching subjects). If a push is missed - the consumer sequence skips - the consumer is transparently deleted and recreated starting just after the last in-order message; the out-of-order message that exposed the gap is **discarded** (not forwarded), and the recreated consumer replays the missing range in order. Delivery to your callback therefore stays in order and gap-free, with no duplicates and no recreate storm. If the restart point has been pruned/expired, recovery resumes from the next available message. If the recreate itself fails (for example the stream was deleted or a leadership change is in progress), the error is contained to this ordered consumer rather than disrupting delivery to other subscriptions on the connection.

A separate idle-heartbeat watchdog covers the case the sequence-gap logic cannot: because the gap check only runs when a frame arrives, a consumer that is reaped (for example after an `inactive_threshold` lapse or a server-side ordered-consumer restart) and then stops delivering entirely would otherwise leave a live subscription to a deliver inbox nothing will ever publish to again - no data, no heartbeat, no error, forever. The ordered consumer requests a periodic `idle_heartbeat`, and if no frame (data, heartbeat, or flow-control) arrives for two intervals the watchdog transparently recreates it from the last in-order point - the same recovery the gap path uses - firing at most once per silence episode. Tune the interval with the optional `idleHeartbeatNs` argument to `subscribeOrderedConsumer()`. If the recreate collides with a connection drop it is deferred rather than treated as terminal: the watchdog retries it with a fresh attempt budget once the connection is open again, so a reconnect blip does not kill the consumer.

A caller-owned push consumer created with `idle_heartbeat` is watched too, but because the library cannot recreate a consumer it does not own, a stall there surfaces a descriptive "not active" error through the `errorListener` instead. Those consumers also get the ADR-9 heartbeat gap check: each idle heartbeat's `Nats-Last-Consumer` is compared against the sequence delivered locally, and a mismatch (including a consumer replaced server-side, which resets the sequence) is reported once per episode through the `errorListener` and the logger.

The same watchdog protects KV **and Object Store** watches, which both ride ordered consumers and request a default `idle_heartbeat` (tunable via `KeyWatchOptions::$idleHeartbeat` and `ObjectStoreWatchOptions::$idleHeartbeat`), so a slow-consumer drop, a reconnect window, or a reaped watch consumer is replayed from the last seen revision instead of becoming a silent permanent gap. Stop a watch (or any ordered consumer) with `JetStreamContext::stopOrderedConsumer($sid)`, which resolves the current subscription even after recreates rotated it; a plain `unsubscribe()` only works until the first recreate.

## Production Notes and Limitations

- **Runtime requirements.** PHP 8.2+ on the async runtime `amphp/amp ^3.1.3` and `amphp/socket ^2.3` (which requires `ext-openssl` for TLS). Amp 3.1.3 is the floor because earlier 3.1 releases report a `CompositeCancellation`'s cancellation only one event-loop tick after one of its parts fired, and with waiting for a reconnect disabled a request whose reply came in the same read as a server's lame-duck `INFO` lost that reply (#200). `ext-sodium` is additionally required for NKey / JWT authentication, and `ext-zlib` for WebSocket permessage-deflate compression; both are optional otherwise (declared under `suggest`). The library is async-first; it does **not** require Swoole/ReactPHP or `ext-sockets`.
- **Concurrency model.** Message delivery, request replies, and JetStream pull/push consumption are driven by reads. An application must run a `processIncoming()` loop (directly, or indirectly via helpers such as `request()`, `flush()`, `SubscriptionQueue::next()`, or the consumer iterators, which pump it for you) for callbacks to fire. An idle, publisher-only connection stays alive on its own because the heartbeat timer self-reads `PONG`s - see [Heartbeat and Request Timeouts](#heartbeat-and-request-timeouts).
- **One connection per fiber/process boundary.** A `NatsConnection` serializes its writes and owns a single socket read; share a connection within a coroutine tree, not across independent concurrent readers.
- **Interoperability.** KeyValue and Object Store buckets use the official NATS layouts (`KV_`/`OBJ_` streams, base64url object-name encoding, `SHA-256=`-prefixed base64url digests), so buckets written by this client are readable by the `nats` CLI and other official clients, and vice-versa.
- **Observability.** Pass a PSR-3 `LoggerInterface` via `new NatsOptions(logger: $logger)` to capture lifecycle events (connect, disconnect, reconnect, close, server discovery, lame-duck), per-attempt reconnect/backoff, and async errors. It defaults to a `NullLogger`. For structured, programmatic hooks (metrics, alerting, circuit breakers) without parsing log strings, pass typed closures instead: `connectionListener: Closure(ConnectionEvent $event, ?Throwable $error): void` is invoked on connection-lifecycle transitions, and `errorListener: Closure(Throwable $error): void` on async errors. The connection listener hears of a connection only while it is still the open one: a connection closed, being closed, replaced or lost before the listener was told it opened is not announced, and when it was lost, neither is its loss, though a `Closed` still is. Only the logger records every transition - see [Reconnect Behavior](#reconnect-behavior). Exceptions thrown by a listener are swallowed so a faulty hook cannot disrupt the connection; so are those a logger throws while it logs a lifecycle transition, a failed reconnect attempt, or any error it reports - and such an error still reaches the `errorListener`. _Verified by: [NatsConnectionTest::testLoggerCapturesLifecycleEvents](tests/Unit/NatsConnectionTest.php)._
- **Server version requirements.** Newer features (per-message TTL, atomic batch publish, scheduled publish, priority groups, counters, batched Direct Get) require recent NATS servers - see [NATS Server Version Requirements](#nats-server-version-requirements).
- **An `-ERR` can fail an unrelated operation.** An operation's read (a `request()`, a JetStream call, a polling queue) delivers messages for every subscription on the connection. Another subscription's handler that throws there is reported and the operation completes (see [Handler Failures](#handler-failures)), but an `-ERR` the server keeps the connection open for, such as its rejection of another subscription's SUB, fails the operation whose read meets it, even when its own result has arrived: the `-ERR` names no subscription. A service's `run()` reports it instead. See [Reconnect Behavior](#reconnect-behavior).
- **Not yet implemented.** A dedicated high-throughput fast-ingest batch publisher ([#12](https://github.com/ideaconnect/php-nats-jetstream-client/issues/12)) is tracked but blocked on an upstream reference; standard JetStream publish with in-flight pipelining is available today and is sufficient for most workloads.

## Configuration Option Mapping

`NatsOptions` fields and defaults (every default below is asserted by [NatsOptionsTest::testDefaultsMatchDocumentedValues](tests/Unit/NatsOptionsTest.php)):

| Option | Type | Default | Notes |
| --- | --- | --- | --- |
| `servers` | `list<string>` | `['nats://127.0.0.1:4222']` | Supports `nats://` and `tls://` endpoints. |
| `name` | `string` | `idct-php-nats-client` | Sent in CONNECT payload. |
| `inboxPrefix` | `string` | `_INBOX` | Prefix for generated request inbox subjects. |
| `connectTimeoutMs` | `int` | `5000` | Transport connect timeout in milliseconds. |
| `requestTimeoutMs` | `int` | `10000` | Default request/reply timeout. |
| `reconnectEnabled` | `bool` | `true` | Enables reconnect flow. |
| `maxReconnectAttempts` | `int` | `10` | Max reconnect attempts before closing. |
| `reconnectDelayMs` | `int` | `100` | Base reconnect backoff delay. |
| `reconnectMaxDelayMs` | `int` | `10000` | Maximum reconnect delay (caps exponential backoff). |
| `reconnectJitterMs` | `int` | `50` | Random jitter added to reconnect delay. |
| `pingIntervalSeconds` | `int\|float` | `30` | Client heartbeat interval in seconds; fractional values allowed, `0` disables. |
| `maxPingsOut` | `int` | `2` | Max outstanding pings before failure. |
| `verbose` | `bool` | `false` | NATS verbose protocol mode. |
| `pedantic` | `bool` | `false` | NATS pedantic protocol mode. |
| `noEcho` | `bool` | `false` | Suppresses server echo of own published messages. |
| `tlsRequired` | `bool` | `false` | Forces TLS context in transport. |
| `tlsHandshakeFirst` | `bool` | `false` | When `true`, performs the TLS handshake immediately after connecting (before server INFO). When `false` (default), the client uses the standard NATS flow: read the plaintext INFO, then upgrade to TLS when TLS is required (option, `tls://` scheme, or server `tls_required`). |
| `tlsCaFile` | `?string` | `null` | CA bundle path for peer verification. |
| `tlsCertFile` | `?string` | `null` | Client certificate path. |
| `tlsKeyFile` | `?string` | `null` | Client private key path. |
| `tlsKeyPassphrase` | `?string` | `null` | Passphrase for encrypted key file. |
| `tlsPeerName` | `?string` | `null` | Overrides TLS peer name (SNI/verification). |
| `tlsVerifyPeer` | `bool` | `true` | Enables certificate verification. |
| `token` | `?string` | `null` | Token auth, encoded as `auth_token`. |
| `username` | `?string` | `null` | Username auth field. |
| `password` | `?string` | `null` | Password auth field. |
| `jwt` | `?string` | `null` | JWT user credential. |
| `nkey` | `?string` | `null` | Public NKey for JWT auth mode or standalone NKey challenge-response auth. |
| `nonceSigner` | `?NonceSignerInterface` | `null` | Signs the server nonce for JWT or standalone NKey auth. |
| `maxPendingMessagesPerSubscription` | `int` | `1024` | Messages buffered per subscription until its handler takes them. See [Slow Consumers](#slow-consumers). |
| `slowConsumerPolicy` | `SlowConsumerPolicy` | `DropOldest` | One of `DropOldest`, `DropNewest`, `Error`: what happens to a message that finds its subscription's buffer full, and how you learn about it. See [Slow Consumers](#slow-consumers). |
| `connectionListener` | `?Closure(ConnectionEvent,?Throwable):void` | `null` | Typed hook for connection-lifecycle transitions (connect/disconnect/reconnect/close/discovery/lame-duck). Listener exceptions are swallowed. |
| `errorListener` | `?Closure(Throwable):void` | `null` | Typed hook for async errors. Listener exceptions are swallowed. |
| `jwtProvider` | `?Closure():string` | `null` | Supplies the JWT at connect time (e.g. for credential rotation), overriding `jwt`. |
| `tokenProvider` | `?Closure():string` | `null` | Supplies the auth token at connect time (e.g. for credential rotation), overriding `token`. |
| `reconnectBufferSize` | `int` | `8388608` | Max bytes of outbound publishes buffered while reconnecting; flushed on a successful reconnect. `0` disables buffering (publishes while disconnected throw). 8 MiB, matching nats.go. |
| `tlsContext` | `?ClientTlsContext` | `null` | Escape hatch: a pre-built Amp TLS context used verbatim for the handshake (in-memory PEM, ALPN, custom verification). When set, the connection is treated as TLS-required. |
| `randomizeServers` | `bool` | `false` | Shuffle the configured server pool once at construction so a client fleet spreads its initial connections across the cluster. |
| `retryOnFailedInitialConnect` | `bool` | `false` | Retry the very first connection (up to `maxReconnectAttempts`, with backoff) when it fails, even if `reconnectEnabled` is off. |
| `webSocketHeaders` | `array<string,string>` | `[]` | Extra headers sent on the WebSocket upgrade request (only used by the WebSocket transport). |
| `webSocketCompression` | `bool` | `false` | Negotiate permessage-deflate on the WebSocket transport (requires `ext-zlib`). |
| `logger` | `?Psr\Log\LoggerInterface` | `null` | PSR-3 logger for lifecycle/reconnect/error events; defaults to a `NullLogger`. |
| `readChunkSizeBytes` | `int` | `131072` | Max bytes a single transport socket read may return (Amp socket chunk size), up from Amp's 8 KiB default. Larger reads divide the per-chunk syscall/fiber/parser overhead for large payloads without forcing larger reads, so small messages are unaffected. Must be at least 1. |
| `waitForReconnect` | `bool` | `true` | While a reconnect is in flight, `request()`/`requestWithHeaders()`/`requestMany()`, `subscribe()`, `flush()`, `rtt()`, `processIncoming()`/`readIncoming()`, `drain()` and `drainSubscription()` wait for it within their own timeout instead of failing with `Connection is not open`, and a publish buffered meanwhile yields one event-loop tick - which is what lets a synchronous application drive the reconnect. `false` fails fast, except that `drain()` then closes the connection and throws, and `drainSubscription()` delivers what already arrived and removes the subscription. See [Reconnect Behavior](#reconnect-behavior). |
| `slowConsumerErrorsFailOperations` | `bool` | `false` | With `SlowConsumerPolicy::Error`: `true` lets an overflow of any subscription fail the operation whose read ran into it - even a `request()` whose reply had already arrived - as before this option existed. By default such an operation reports the overflow through the `errorListener` and completes. `Service::run()` reports it either way. See [Slow Consumers](#slow-consumers). |
| `handlerErrorsFailOperations` | `bool` | `false` | `true` lets a subscription handler that throws fail the operation whose read ran it - even a `request()` whose reply had already arrived - as before this option existed, when a handler's `CancelledException` could also end an operation's wait early (unless the operation's own result arrived in the same read behind it, which is delivered first and completes it). By default such an operation reports the handler's exception through the `errorListener`, delivers the rest of its read and completes; a handler of the operation's own subscription still fails it. `processIncoming()` and `readIncoming()` throw it, and `Service::run()` reports it, either way. See [Handler Failures](#handler-failures). |

## Performance Benchmark Recipe

Quick local publish/request benchmark (single process).

The responder is pumped by a single long-lived background loop rather than one
`processIncoming()` call per request. `processIncoming()` consumes one transport chunk, and TCP
can coalesce several protocol messages into one chunk, so a one-call-per-request responder
desynchronizes and stalls. A continuous read loop (cancelled when the run finishes) avoids that:

```php
<?php

declare(strict_types=1);

use Amp\CancelledException;
use Amp\DeferredCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use function Amp\async;

$iterations = 5000;
$subject = 'bench.echo';

$server = new NatsClient(new NatsOptions());
$client = new NatsClient(new NatsOptions());

$server->connect()->await();
$client->connect()->await();

$server->subscribe($subject, static function (NatsMessage $message) use ($server): void {
	if ($message->replyTo !== null) {
		$server->publish($message->replyTo, 'ok')->await();
	}
})->await();

// Drive the responder from one continuous background read loop.
$serverCancel = new DeferredCancellation();
$serverLoop = async(static function () use ($server, $serverCancel): void {
	$cancellation = $serverCancel->getCancellation();
	while (!$cancellation->isRequested()) {
		try {
			$server->processIncoming($cancellation)->await();
		} catch (CancelledException) {
			break;
		}
	}
});

$start = hrtime(true);
for ($i = 0; $i < $iterations; $i++) {
	$client->request($subject, 'x', 2000)->await();
}
$elapsedNs = hrtime(true) - $start;

$serverCancel->cancel();
$serverLoop->await();

$totalMs = $elapsedNs / 1_000_000;
$rps = $iterations / max(0.001, ($elapsedNs / 1_000_000_000));

echo 'iterations=' . $iterations . PHP_EOL;
echo 'total_ms=' . number_format($totalMs, 2, '.', '') . PHP_EOL;
echo 'req_per_sec=' . number_format($rps, 2, '.', '') . PHP_EOL;

$client->disconnect()->await();
$server->disconnect()->await();
```

A ready-to-run version of this recipe ships at [`scripts/benchmark.php`](scripts/benchmark.php) (it also measures fire-and-forget publish throughput):

```bash
docker compose up -d nats
NATS_URL=nats://127.0.0.1:14222 BENCH_ITER=5000 php scripts/benchmark.php
docker compose down
```

### Sample baseline

Single process, single connection, loopback. Numbers are environment-specific (CPU, loopback vs network, server config) and are a relative baseline, not a guarantee.

| Metric | Result |
| --- | --- |
| Request/reply (synchronous round-trip) | ~800 req/s (~1.25 ms/req) |
| Fire-and-forget publish (each awaited) | ~15,000 msg/s |

_Measured with PHP 8.5, `nats-server` 2.12.9, 5,000 iterations, 16-byte payloads, on a WSL2 loopback. Request/reply is latency-bound (each request awaits its reply on one connection); both publish and request loops await every call, so these reflect serialized per-call throughput rather than peak pipelined rates._

## Testing

Typical local workflow:

```bash
composer install
composer test:unit
composer test:bdd
composer stan
composer test:e2e
```

Additional useful commands:

```bash
composer test
RUN_INTEGRATION=1 composer test:integration
composer test:bdd
BEHAT_SUITE=core composer test:bdd
composer test:integration:repeat
composer fixture:jwt:check
composer fixture:jwt
composer fix
composer infection
```

`composer infection` runs [Infection](https://infection.github.io/) mutation testing against the `unit` testsuite (no Docker; needs a coverage driver - Xdebug or PCOV). It is a quality gate on top of line coverage: it makes small changes ("mutants") to `src/` and fails if the tests don't catch them. The suite scores ~94% Covered MSI and CI enforces a strict 90% floor via the dedicated `mutation` job. The strictness is overridable for local exploration, e.g. `INFECTION_MIN_MSI=0 composer infection -- --show-mutations`, and `INFECTION_DIFF_BASE=origin/main composer infection` mutates only your changed lines.

Infection requires **PHP 8.3+** and is intentionally **not** in `require-dev` (so the library stays installable on PHP 8.2). The CI `mutation` job installs it on the fly; to run mutation testing locally, add it first: `composer require --dev infection/infection:^0.33`.

`composer test:e2e` is the preferred compose-backed validation path. It checks the committed JWT fixtures, starts the local NATS stack, waits for readiness, runs unit tests, runs integration tests, runs the Behat feature suite, and tears the stack down again. `SKIP_UNIT_TESTS=1 composer test:e2e` leaves out the unit stage; CI sets it, because its coverage step and the Unit + Static jobs already run the unit suite.

The compose servers run without debug or trace logging. To trace one while debugging, add `"-DV"` to its `command` in `docker-compose.yml` and recreate it with `docker compose up -d --force-recreate <name>`.

`composer test:bdd` runs only the Behat feature suite against the same local Docker Compose fixtures. Use `BEHAT_SUITE=core composer test:bdd` to run a narrower slice while iterating locally, or `BEHAT_SUITE=core composer test:e2e` to keep the rest of the e2e flow and narrow only the Behat stage.

Base integration endpoint:

- `NATS_URL` (default: `nats://127.0.0.1:14222`)

When you run `docker compose up -d` in this repository, additional local auth fixtures are available by default:

- token auth: `nats://127.0.0.1:14223` with token `local-test-token`
- username/password auth: `nats://127.0.0.1:14224` with `local-user` / `local-pass`
- TLS handshake-first auth: `tls://127.0.0.1:14225` using the generated files under `build/tls/`
- JWT auth: `nats://127.0.0.1:14227` using the generated operator/account resolver chain under `build/nats/jwt/`
- standalone NKey auth: `nats://127.0.0.1:14226` with seed `SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY`

The integration tests use those defaults automatically. Override them with environment variables when you want to target external infrastructure instead.

To regenerate the committed local JWT fixture artifacts and resolver config intentionally, run:

```bash
composer fixture:jwt
```

If the local `nats-jwt` compose service is already running, the regeneration script recreates it so the server picks up the new operator/account resolver state immediately.

To verify the committed JWT fixture is in sync with the regeneration script, run:

```bash
composer fixture:jwt:check
```

- `NATS_TLS_URL`: TLS-enabled server URL used by `testTlsHandshakeFirstConnection`
- `NATS_TLS_CA_FILE`: optional CA bundle path for TLS verification
- `NATS_TLS_CERT_FILE`: optional client certificate path for TLS/mTLS
- `NATS_TLS_KEY_FILE`: optional client private key path for TLS/mTLS
- `NATS_TLS_SKIP_VERIFY`: set to `1` to disable peer verification in the TLS integration test
- `NATS_TOKEN_URL`: token-auth server URL used by `testTokenAuthSuccessAndFailure`
- `NATS_TOKEN`: valid token for the token-auth endpoint
- `NATS_TOKEN_INVALID`: invalid token used for the negative token-auth path
- `NATS_USERPASS_URL`: username/password-auth server URL used by `testUserPasswordAuthSuccessAndFailure`
- `NATS_USERNAME`: valid username for the user/password endpoint
- `NATS_PASSWORD`: valid password for the user/password endpoint
- `NATS_BAD_PASSWORD`: invalid password used for the negative user/password path
- `NATS_JWT_URL`: JWT-auth server URL used by `testJwtNonceAuthenticationFlow` (default: `nats://127.0.0.1:14227`)
- `NATS_JWT`: user JWT presented in the CONNECT payload (defaults to `build/nats/jwt/user.jwt`)
- `NATS_JWT_NKEY_SEED`: encoded user seed used by `NkeySeedSigner` to derive the public NKey and sign the server nonce (defaults to `build/nats/jwt/user.seed`)
- `NATS_NKEY_URL`: standalone NKey-auth server URL used by `testStandaloneNkeyAuthenticationFlow`
- `NATS_NKEY_SEED`: encoded user seed used by `NkeySeedSigner` for standalone NKey challenge-response auth

Example overrides for external infrastructure:

```bash
# Base server override.
RUN_INTEGRATION=1 \
NATS_URL=nats://demo.example.net:4222 \
composer test:integration

# Token auth override.
RUN_INTEGRATION=1 \
NATS_TOKEN_URL=nats://token.example.net:4222 \
NATS_TOKEN=prod-token-value \
NATS_TOKEN_INVALID=wrong-token \
./vendor/bin/phpunit --testsuite integration --filter testTokenAuthSuccessAndFailure

# Username/password override.
RUN_INTEGRATION=1 \
NATS_USERPASS_URL=nats://auth.example.net:4222 \
NATS_USERNAME=alice \
NATS_PASSWORD=s3cr3t \
NATS_BAD_PASSWORD=wrong-pass \
./vendor/bin/phpunit --testsuite integration --filter testUserPasswordAuthSuccessAndFailure

# TLS override with strict verification.
RUN_INTEGRATION=1 \
NATS_TLS_URL=tls://tls.example.net:4222 \
NATS_TLS_CA_FILE=/path/to/ca.pem \
NATS_TLS_CERT_FILE=/path/to/client-cert.pem \
NATS_TLS_KEY_FILE=/path/to/client-key.pem \
./vendor/bin/phpunit --testsuite integration --filter 'testTlsHandshakeFirstConnection|testTlsConnectionFailsWithoutClientCertificate|testTlsConnectionFailsWithWrongCa|testTlsConnectionFailsWithPeerNameMismatch'

# JWT auth override.
RUN_INTEGRATION=1 \
NATS_JWT_URL=nats://jwt.example.net:4222 \
NATS_JWT="$(cat /path/to/user.jwt)" \
NATS_JWT_NKEY_SEED="$(cat /path/to/user.seed)" \
./vendor/bin/phpunit --testsuite integration --filter testJwtNonceAuthenticationFlow

# Standalone NKey auth override.
RUN_INTEGRATION=1 \
NATS_NKEY_URL=nats://nkey.example.net:4222 \
NATS_NKEY_SEED="$(cat /path/to/user.seed)" \
./vendor/bin/phpunit --testsuite integration --filter testStandaloneNkeyAuthenticationFlow
```

Focused auth/TLS integration run:

```bash
RUN_INTEGRATION=1 ./vendor/bin/phpunit --testsuite integration --filter 'testTlsHandshakeFirstConnection|testTlsConnectionFailsWithoutClientCertificate|testTlsConnectionFailsWithWrongCa|testTlsConnectionFailsWithPeerNameMismatch|testTokenAuthSuccessAndFailure|testUserPasswordAuthSuccessAndFailure|testJwtNonceAuthenticationFlow|testStandaloneNkeyAuthenticationFlow'
```

To do a quick local flake check against the compose-backed environment, run:

```bash
composer test:integration:repeat
```

CI has no soak job: repeat the integration suite locally with `composer test:integration:repeat`.

## Contributing and contributors

Contributions should keep changes focused and paired with the narrowest useful verification.

- Add or update tests when behavior changes.
- Prefer `composer test:unit` for small local changes and `composer test:e2e` for auth, transport, protocol, JetStream, or integration-fixture changes.
- Do not hand-edit generated JWT fixture files under `build/nats/jwt/`; regenerate them with `composer fixture:jwt`.
- Run `composer stan` for code changes and `composer fix` when style adjustments are needed.
- Review `AGENTS.md` for repository structure, standards, and continuation guidance before larger changes.

Many thanks for all the contributions:
* [@amirdzhanyane-bit](https://www.github.com/amirdzhanyane-bit) [Protocol messages fragmentation issue](https://github.com/ideaconnect/php-nats-jetstream-client/issues/2)

## Current Test Baseline

- Unit tests cover protocol encoding/parsing, handshake/state transitions, subscriptions, backpressure policies, request/reply flows, reconnect/server-rotation behavior, and exponential backoff.
- Unit tests also cover JetStream account info, stream and consumer CRUD, publish acknowledgments, API error mapping, fetch batch, ordered consumers, consumer pause/resume, KV bucket options, ObjectStore chunking and digest verification.
- Unit tests cover microservices framework including PING/INFO/STATS/SCHEMA discovery and grouped endpoint hierarchy.
- Integration tests cover live connect/disconnect, publish-subscribe roundtrip, request-reply, connection rotation fallback, JetStream stream/consumer lifecycle with publish-ack flow, KV operations, ObjectStore operations, and service discovery.
- Integration tests also cover local token auth, username/password auth, TLS handshake-first auth including strict peer-validation, hostname mismatch, and missing-client-cert failures, resolver-backed JWT auth, and standalone NKey auth.
- Static analysis runs with PHPStan level 8.
- Combined unit + integration line coverage is ~99.4%, enforced at a 97% floor in CI (the build fails below it).
- Mutation testing (Infection) scores ~94% Covered MSI, enforced at a 90% floor in CI.
- The suite is catalogued file by file, one line per test, in [TESTS.md](TESTS.md).

## License

This project is licensed under the **BSD 3-Clause License** - see the [LICENSE](LICENSE) file for the full text. Copyright © IDCT - Bartosz Pachołek.
