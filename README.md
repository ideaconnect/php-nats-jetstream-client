# IDCT PHP NATS JetStream Client

[![codecov](https://codecov.io/gh/ideaconnect/php-nats-jetstream-client/graph/badge.svg?token=A816f4EXon)](https://codecov.io/gh/ideaconnect/php-nats-jetstream-client)
[![CI](https://github.com/ideaconnect/php-nats-jetstream-client/actions/workflows/ci.yml/badge.svg)](https://github.com/ideaconnect/php-nats-jetstream-client/actions/workflows/ci.yml)
[![Made in EU](https://raw.githubusercontent.com/ideaconnect/made-in-eu/main/software-badge/made-in-eu.svg)](https://github.com/ideaconnect/made-in-eu)

Async-first NATS and JetStream client for PHP 8.3+ with first-class support for core NATS messaging, JetStream, KeyValue, ObjectStore, and NATS microservices.

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
- JetStream stream message get by sequence — both the regular `STREAM.MSG.GET` request and the Direct Get API (`directGetStreamMessage()` / `directGetLastMessageForSubject()`)
- JetStream atomic (all-or-nothing) batch publish (`batch()` → `BatchPublisher`, ADR-50; requires `allow_atomic`, NATS 2.12+)
- Scheduled publish (`@at` support)
- Distributed counter CRDT (atomic `incrementCounter()` / `counterValue()`, arbitrary-precision values)
- KeyValue API (bucket lifecycle with history/TTL/storage options, put/get/update/delete/purge, watch, getAll/status)
- ObjectStore API (bucket lifecycle, put/get/delete/list/watch, chunked uploads, streaming upload via `putStream()`, SHA-256 digest verification)
- Connection `flush()` (PING/PONG round-trip) to confirm the server has processed prior writes
- Request-many scatter-gather (`requestMany()`: collect multiple replies bounded by max-count / stall / timeout)
- Connection traffic statistics (`statistics()` → `ConnectionStats`) and round-trip-time measurement (`rtt()`)
- Microservices framework (service registration, PING/INFO/STATS/SCHEMA discovery, grouped endpoints)
- Server authorization methods: token, username/password, JWT + nonce signer, built-in NKey seed signer, credentials file parser
- Standalone NKey authentication (Ed25519 challenge signing without JWT)
- `no_echo` CONNECT option
- `tlsHandshakeFirst` TLS option
- Typed JetStream configuration enums (RetentionPolicy, StorageBackend, DiscardPolicy, DeliverPolicy, AckPolicy, ReplayPolicy)
- Max frame size limit in protocol parser (DoS protection)
- Queue-based polling subscribe API (`SubscriptionQueue` with `fetch()`, `next()`, `fetchAll()`)
- Pull-consumer batching/iteration chain API (`PullConsumerIterator` with `setBatching()`, `setIterations()`, `handle()`)
- Stream mirroring and sourcing configuration helpers (`StreamSource`)
- Republish and subject transform configuration helpers (`Republish`, `SubjectTransform`)

Scheduling note: scheduled messages use the NATS scheduler headers (ADR-51) and accept `@at`, `@every`, 6-field cron, and the predefined aliases (`@daily`, `@hourly`, …). Build expressions with `IDCT\\NATS\\JetStream\\Schedule::at(...)`, `Schedule::atTimestamp(...)`, `Schedule::every(...)`, `Schedule::cron(...)`, or `Schedule::predefined(...)`. The target stream must be created with `allow_msg_schedules` (NATS 2.12+).

## NATS Server Version Requirements

Most of this library works against any JetStream-enabled server. Some features depend on newer NATS
server versions; the table below lists the minimum version per feature. **You do not need to check the
version yourself** — if you use a feature against a server that is too old, the request fails fast with
an `IDCT\NATS\Exception\UnsupportedFeatureException` (a subclass of `JetStreamException`) carrying the
feature name, the required version, and the version the server reported. The check is reactive (derived
from the server's own error response), so there is no per-request version probe.

| Feature | API | Min NATS | Server config / header |
| --- | --- | --- | --- |
| Multi-subject consumer filters | `createConsumer(..., ['filter_subjects' => [...]])` | 2.10 | `filter_subjects` |
| Per-message / KV TTL | `publish(..., ttl:)`, `KeyValueBucket::put/delete/purge(..., ttl:)` | 2.11 | `allow_msg_ttl`, `Nats-TTL` |
| Subject delete markers | KV/Object Store `watch()`/`get()` (handled automatically) | 2.11 | `subject_delete_marker_ttl`, `Nats-Marker-Reason` |
| Pull priority groups | `fetchBatch(..., $pull)`, `PullConsumerIterator::setGroup/setPriority/...`, `unpinConsumer()` | 2.11 (`prioritized` 2.12) | `priority_groups`/`priority_policy`, `Nats-Pin-Id` |
| Batched / multi Direct Get | `directGetBatch()`, `directGetLastForSubjects()` | 2.11 | `allow_direct` |
| Scheduled publishing (`@every`/cron/aliases) | `publishScheduled()`, `Schedule::every/cron/predefined` | 2.12 | `allow_msg_schedules`, `Nats-Schedule*` |
| Atomic batch publish | `batch()` → `BatchPublisher` | 2.12 | `allow_atomic`, `Nats-Batch-*` |
| Distributed counter CRDT | `incrementCounter()`, `counterValue()` | 2.12 | `allow_msg_counter`, `Nats-Incr` |
| Publish de-duplication | `publish(..., msgId:)` | 2.2 | `Nats-Msg-Id` |

```php
use IDCT\NATS\Exception\UnsupportedFeatureException;

try {
    $js->batch()->add('orders.created', $payload)->commit()->await();
} catch (UnsupportedFeatureException $e) {
    // e.g. "The "allow_atomic" feature requires NATS server 2.12+, but the connected server reports 2.10.5."
    echo "{$e->feature} needs NATS {$e->requiredVersion}; server is {$e->serverVersion}\n";
}
```

You can also query the requirement programmatically: `IDCT\NATS\JetStream\FeatureSupport::requiredVersion('allow_atomic')` returns `"2.12"`.

## 🚀 This project looks for funding. Love my work? Support it! 💖

* ☕ **Buy me a coffee**: https://buymeacoffee.com/idct

* 💝 **Sponsor**: https://github.com/sponsors/ideaconnect

* 🪙 **BTC**: bc1qntms755swm3nplsjpllvx92u8wdzrvs474a0hr

* 💎 **ETH**: 0x08E27250c91540911eD27F161572aFA53Ca24C0a

* ⚡ **TRX**: TVXWaU4ScNV9RBYX5RqFmySuB4zF991QaE

* 🚀 **LTC**: LN5ApP1Yhk4iU9Bo1tLU8eHX39zDzzyZxB

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

By default `NatsClient` uses the TCP transport (`AmpSocketTransport`). To connect over WebSocket — e.g. through a NATS gateway that only exposes `ws://` / `wss://` — construct a `WebSocketTransport` and inject it. The `ws://` / `wss://` endpoint goes in `servers`; `wss://` negotiates TLS during connect (using the same `tls*` options), and the optional `webSocketHeaders` / `webSocketCompression` options apply to the upgrade handshake.

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

_Verified by: [NatsClientTest::testClientPublishWithHeadersAndRequestWithHeaders](tests/Unit/NatsClientTest.php), [NatsClientTest::testClientConnectAndPublishDelegatesToConnection](tests/Unit/NatsClientTest.php); [features/core/headers_queueing.feature](features/core/headers_queueing.feature)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$client->publishWithHeaders('events.orders', '{"id":123}', [
	'Nats-Msg-Id' => 'orders-123',
	'Content-Type' => 'application/json',
])->await();

$reply = $client->requestWithHeaders('svc.echo', 'hello', [
	'X-Request-Id' => 'req-123',
], 2000)->await();

echo $reply->payload . PHP_EOL;
echo $client->serverInfo()?->serverName . PHP_EOL;

$client->disconnect()->await();
```

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

_Verified by: [SubscriptionQueueTest](tests/Unit/SubscriptionQueueTest.php) (`fetch`/`next`/`fetchAll`/`setTimeout`); [NatsClientIntegrationTest::testSubscriptionQueuePollingDeliversLive](tests/Integration/NatsClientIntegrationTest.php); [features/core/headers_queueing.feature](features/core/headers_queueing.feature)._

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

// Non-blocking fetch — returns null if nothing available.
$msg = $queue->fetch();

// Blocking fetch — waits up to the configured timeout, returns null on timeout.
// With no timeout configured it performs a single processIncoming() cycle (like fetch()).
$msg = $queue->next();

// Batch fetch — collects up to 10 messages within the timeout window.
$messages = $queue->fetchAll(limit: 10);

$client->unsubscribe($queue->sid)->await();
$client->disconnect()->await();
```

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

$kv = $client->jetStream()->keyValue('cfg');
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

$client->unsubscribe($watchSid)->await();
$kv->deleteBucket()->await();
$client->disconnect()->await();
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

_Verified by: [ObjectStoreBucketTest](tests/Unit/ObjectStoreBucketTest.php) (`testSeal`, `testAddLink`, `testAddBucketLink`, `testUpdateMetaRenamesPreservingNuid`, `testUpdateMetaReplacesMetadataInPlace`)._

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$store = $client->jetStream()->objectStore('assets');

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
// incrementally — the streaming counterpart to getToCallback().
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

Services expose runtime helpers: `statsSnapshot()` returns the current `io.nats.micro.v1.stats_response` array (per-endpoint `num_requests` / `num_errors` / `last_error` / `processing_time` / `average_processing_time`), `reset()` zeroes those counters, and `withRequestValidator()` enables opt-in per-request validation for endpoints declared with a `schema`. The validator receives the message and the endpoint schema and returns `null` to accept or a string rejection reason (which becomes a `VALIDATION_ERROR` error reply).

_Verified by: [ServiceTest](tests/Unit/ServiceTest.php) (`testStatsIncludeDetailedMetrics`, `testResetClearsStats`, `testRequestValidatorCanRejectRequests`)._

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

> 📄 **Runnable example:** [`examples/graceful-drain.php`](examples/graceful-drain.php)

_Verified by: [NatsConnectionTest](tests/Unit/NatsConnectionTest.php) (`testDrainUnsubscribesAllAndCloses`, `testDrainDeliversBufferedMessagesBeforeClosing`); [features/resilience/client_resilience.feature](features/resilience/client_resilience.feature)._

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

$client->unsubscribe($sid)->await();
$js->deleteStream('EVENTS')->await();
$client->disconnect()->await();
```

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

// Pause the consumer until a specific time (ISO 8601 format).
$js->pauseConsumer('ORDERS', 'PROC', '2026-03-12T00:00:00Z')->await();

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
`Nats-Num-Pending: 0`); the wait is bounded by `$expiresMs` (default 5000) so a silent server cannot
hang the call. `directGetBatch()` returns a `list<NatsMessage>`; an error status (e.g. `408`) is
raised as a `JetStreamException`.

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

_Verified by: [JetStreamContextTest](tests/Unit/JetStreamContextTest.php) (`testDirectGetBatchCollectsUntilEob`, `testDirectGetLastForSubjects`, `testDirectGetBatchSurfacesError`, `testDirectGetLastForSubjectsWithEmptySubjectsReturnsEmpty`, `testDirectGetLastForSubjectsRejectsWildcardSubjectWithStar`, `testDirectGetLastForSubjectsRejectsWildcardSubjectWithGreaterThan`, `testDirectGetBatchRejectsZeroExpiresMs`); [JetStreamIntegrationTest::testJetStreamBatchedDirectGet](tests/Integration/JetStreamIntegrationTest.php)._

### Atomic Batch Publish

> 📄 **Runnable example:** [`examples/atomic-batch-publish.php`](examples/atomic-batch-publish.php)

_Verified by: [BatchPublisherTest](tests/Unit/BatchPublisherTest.php) (`testCommitSendsBatchHeadersAndParsesAck`, `testCommitRejectedAtStart`, `testCommitAbortSurfacesError`, `testCommitEmptyBatchThrows`, `testBatchRejectsOversizedId`, `testAddAfterCommitThrows`, `testCountReturnsNumberOfStagedMessages`, `testBatchIdReturnsConstructedId`); [JetStreamIntegrationTest::testJetStreamAtomicBatchPublish](tests/Integration/JetStreamIntegrationTest.php)._

`$js->batch()` opens an atomic (all-or-nothing) JetStream publish batch (ADR-50). Messages are staged
with the fluent `add($subject, $payload, $headers = [])` and sent together on `commit()`, which returns
a `Future<PubAck>`. Every message carries a shared `Nats-Batch-Id` and an incrementing
`Nats-Batch-Sequence`; the final message carries `Nats-Batch-Commit: 1`, on which the server atomically
commits the whole batch and replies with a single `PubAck` whose `batchCount` is the committed count and
whose `batchId` echoes the batch id. If any consistency check fails the entire batch is aborted and
nothing is stored — the failure surfaces as a `JetStreamException`.

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

> 📄 **Runnable example:** [`examples/pull-consumer-batching-iteration.php`](examples/pull-consumer-batching-iteration.php)

_Verified by: [PullConsumerIteratorTest](tests/Unit/PullConsumerIteratorTest.php); [JetStreamIntegrationTest::testJetStreamPullIteratorBatching](tests/Integration/JetStreamIntegrationTest.php); [features/jetstream-core/consumer_helpers.feature](features/jetstream-core/consumer_helpers.feature)._

The fluent `PullConsumerIterator` wraps `fetchBatch()` with configurable batch size, iterations, and a handler callback:

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

### Pull Consumer Priority Groups

> 📄 **Runnable example:** [`examples/pull-consumer-priority-groups.php`](examples/pull-consumer-priority-groups.php)

_Verified by: [PullConsumerIteratorTest::testHandleRePinsOnStalePin](tests/Unit/PullConsumerIteratorTest.php); [PullConsumerIteratorTest::testBuildPullIncludesAllOptionalFields](tests/Unit/PullConsumerIteratorTest.php); [JetStreamContextTest::testCreateConsumerWithPriorityGroups](tests/Unit/JetStreamContextTest.php); [JetStreamContextTest::testUnpinConsumer](tests/Unit/JetStreamContextTest.php); [JetStreamContextTest::testPinIdOf](tests/Unit/JetStreamContextTest.php)._

ADR-42 priority groups let several pull clients share one consumer while the server steers delivery. Create the consumer with `priority_groups` (a non-empty array of 1..16-character group names) and a `priority_policy` — one of `overflow`, `pinned_client`, or `prioritized`. The priority-group fields require **NATS server 2.11+**; the `prioritized` policy requires **2.12+** (an older server rejects them). See [NATS Server Version Requirements](#nats-server-version-requirements).

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

This repository tracks parity against the basis-company `nats.php` README examples while exposing an Amp-first API tailored to this library.

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

_Verified by: [NatsConnectionTest](tests/Unit/NatsConnectionTest.php) (`testProcessIncomingDispatchesMsgToSubscriber`, `testProcessIncomingUpdatesServerInfoFromAsyncInfoFrame`)._

`processIncoming()` reads a single transport chunk, parses all complete frames from it, and dispatches them to subscription callbacks. It is **non-blocking** — if no data is available, it returns immediately with a frame count of `0`. Because one read returns only a single chunk (and TCP may coalesce several protocol messages into one chunk), call it in a loop to process all available messages:

```php
// Process all available messages for up to 1 second.
$deadline = microtime(true) + 1.0;
while (microtime(true) < $deadline) {
	$frames = $client->processIncoming()->await();
	if ($frames === 0) {
		break;
	}
}
```

The client also applies asynchronous `INFO` updates received after connect, so `serverInfo()` can change during the lifetime of an open connection when the server advertises updated capabilities such as `max_payload` or cluster topology details.

### Heartbeat and Request Timeouts

_Verified by: [NatsConnectionTest](tests/Unit/NatsConnectionTest.php) (`testIdleConnectionStaysOpenViaHeartbeatSelfRead`, `testHeartbeatReadHandlesEmptyErrorAndFatalFrames`, `testRequestTimeoutCancelsReadAndAllowsSubsequentRequest`); live: [NatsClientIntegrationTest](tests/Integration/NatsClientIntegrationTest.php) (`testIdleConnectionStaysOpenViaHeartbeat`, `testRequestTimeoutDoesNotPoisonConnection`, `testMaxPingsOutTriggersReconnect`)._

The heartbeat timer answers its own `PONG`: after sending a `PING` it performs a short, bounded read to consume the reply (unless an application `processIncoming()` read is already in flight). Liveness detection therefore does not depend on the application continuously calling `processIncoming()`, so an otherwise idle connection (for example a pure publisher) is not closed by spurious `maxPingsOut` detection. Only an actual server `PONG` resets the outstanding-ping counter — other inbound frames (data, `INFO`, `PING`) do not — so a server that keeps sending data but stops answering `PING`s is still detected via `maxPingsOut`.

Request and pull-fetch timeouts cancel the underlying socket read rather than leaving it pending. A timed-out `request()` (or `fetchBatch()`/`fetchNext()`) cleanly releases the read, so it cannot orphan an in-flight read or trigger a spurious reconnect on the next operation.

### Reconnect Behavior

_Verified by: [NatsConnectionTest](tests/Unit/NatsConnectionTest.php) (`testBackoffDelayIsExponential`, `testConnectRotatesServersOnReconnectAttempts`, `testReconnectRetriesWhenResubscribeGetsFatalServerError`); live: [NatsClientIntegrationTest](tests/Integration/NatsClientIntegrationTest.php) (`testReconnectAfterTransportLossReplaysSubscriptions`, `testReconnectBackoffDelayProgression`, `testReconnectAttemptsExhaustedReturnsClosed`)._

When a connection drops and `reconnectEnabled` is `true`:

1. **Exponential backoff**: delay is computed as `reconnectDelayMs * 2^(attempt - 1)`, capped at `reconnectMaxDelayMs`, with random jitter up to `reconnectJitterMs`.
2. **Server rotation**: the client cycles through configured servers in order.
3. **Subscription replay**: all active subscriptions are replayed (SUB commands resent) after reconnect.
4. **Replay validation**: reconnect does not treat replayed subscriptions as successful if the server immediately answers with a fatal `-ERR` during replay. In that case reconnect keeps retrying until a healthy server accepts the replay or attempts are exhausted.
5. **Published messages during reconnect are buffered and replayed**: while a reconnect is in flight, publishes are held in an outbound buffer (up to `reconnectBufferSize`, default 8 MiB, matching nats.go) and flushed in order on a successful reconnect. A publish is rejected (throws) only when the buffer is full, when `reconnectBufferSize` is `0` (buffering disabled), or when the connection is closed / not reconnecting. _Verified by: [NatsConnectionTest](tests/Unit/NatsConnectionTest.php) (`testPublishBuffersDuringReconnectAndFlushesOnReconnect`, `testPublishWithHeadersBuffersDuringReconnectAndRecordsOutbound`, `testPublishBufferOverflowThrowsDuringReconnect`)._

Recoverable server `-ERR` frames such as `Invalid Subject` or `Permissions Violation for Publish/Subscription to ...` do not automatically close an already-open connection. Fatal connection-level errors still do.

The initial handshake is bounded by `connectTimeoutMs`, not by a fixed number of transport reads. During bootstrap the client will also answer server `PING` frames and process async `INFO` updates while waiting for the initial `PONG`.

### Ordered Consumer Gap Recovery

_Verified by: [JetStreamContextTest](tests/Unit/JetStreamContextTest.php) (`testSubscribeOrderedConsumerRecreatesOnSequenceGap`, `testSubscribeOrderedConsumerDeliversFilteredMessagesWithoutSpuriousRecreate`); [JetStreamIntegrationTest::testJetStreamOrderedConsumerWithFilteredSubjectAfterPriorMessages](tests/Integration/JetStreamIntegrationTest.php)._

`subscribeOrderedConsumer()` tracks the JetStream **consumer** delivery sequence (which is contiguous per delivery, even for a filtered consumer over a stream that also carries non-matching subjects). If a push is missed — the consumer sequence skips — the consumer is transparently deleted and recreated starting just after the last in-order message; the out-of-order message that exposed the gap is **discarded** (not forwarded), and the recreated consumer replays the missing range in order. Delivery to your callback therefore stays in order and gap-free, with no duplicates and no recreate storm. If the restart point has been pruned/expired, recovery resumes from the next available message. If the recreate itself fails (for example the stream was deleted or a leadership change is in progress), the error is contained to this ordered consumer rather than disrupting delivery to other subscriptions on the connection.

## Production Notes and Limitations

- **Runtime requirements.** PHP 8.3+ on the async runtime `amphp/amp ^3.1` and `amphp/socket ^2.3` (which requires `ext-openssl` for TLS). `ext-sodium` is additionally required for NKey / JWT authentication, and `ext-zlib` for WebSocket permessage-deflate compression; both are optional otherwise (declared under `suggest`). The library is async-first; it does **not** require Swoole/ReactPHP or `ext-sockets`.
- **Concurrency model.** Message delivery, request replies, and JetStream pull/push consumption are driven by reads. An application must run a `processIncoming()` loop (directly, or indirectly via helpers such as `request()`, `flush()`, `SubscriptionQueue::next()`, or the consumer iterators, which pump it for you) for callbacks to fire. An idle, publisher-only connection stays alive on its own because the heartbeat timer self-reads `PONG`s — see [Heartbeat and Request Timeouts](#heartbeat-and-request-timeouts).
- **One connection per fiber/process boundary.** A `NatsConnection` serializes its writes and owns a single socket read; share a connection within a coroutine tree, not across independent concurrent readers.
- **Interoperability.** KeyValue and Object Store buckets use the official NATS layouts (`KV_`/`OBJ_` streams, base64url object-name encoding, `SHA-256=`-prefixed base64url digests), so buckets written by this client are readable by the `nats` CLI and other official clients, and vice-versa.
- **Observability.** Pass a PSR-3 `LoggerInterface` via `new NatsOptions(logger: $logger)` to capture lifecycle events (connect, disconnect, reconnect, close, server discovery, lame-duck), per-attempt reconnect/backoff, and async errors. It defaults to a `NullLogger`. For structured, programmatic hooks (metrics, alerting, circuit breakers) without parsing log strings, pass typed closures instead: `connectionListener: Closure(ConnectionEvent $event, ?Throwable $error): void` is invoked on every connection-lifecycle transition, and `errorListener: Closure(Throwable $error): void` on async errors. Exceptions thrown by a listener are swallowed so a faulty hook cannot disrupt the connection. _Verified by: [NatsConnectionTest::testLoggerCapturesLifecycleEvents](tests/Unit/NatsConnectionTest.php)._
- **Server version requirements.** Newer features (per-message TTL, atomic batch publish, scheduled publish, priority groups, counters, batched Direct Get) require recent NATS servers — see [NATS Server Version Requirements](#nats-server-version-requirements).
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
| `pingIntervalSeconds` | `int` | `30` | Client heartbeat interval setting. |
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
| `maxPendingMessagesPerSubscription` | `int` | `1024` | Slow consumer queue bound per SID. |
| `slowConsumerPolicy` | `SlowConsumerPolicy` | `DropOldest` | One of `DropOldest`, `DropNewest`, `Error`. |
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

`composer infection` runs [Infection](https://infection.github.io/) mutation testing against the `unit` testsuite (no Docker; needs a coverage driver — Xdebug or PCOV). It is a quality gate on top of line coverage: it makes small changes ("mutants") to `src/` and fails if the tests don't catch them. The suite scores ~93% Covered MSI and CI enforces a strict 90% floor via the dedicated `mutation` job. The strictness is overridable for local exploration, e.g. `INFECTION_MIN_MSI=0 composer infection -- --show-mutations`, and `INFECTION_DIFF_BASE=origin/main composer infection` mutates only your changed lines.

`composer test:e2e` is the preferred compose-backed validation path. It checks the committed JWT fixtures, starts the local NATS stack, waits for readiness, runs unit tests, runs integration tests, runs the Behat feature suite, and tears the stack down again.

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

The CI workflow also exposes a manual `workflow_dispatch` soak job named `integration-soak`. When triggered from GitHub Actions, it runs `scripts/repeat-integration.sh` with a configurable repeat count on PHP 8.5.

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
- Combined unit + integration line coverage is ~98%, enforced at a 97% floor in CI (the build fails below it).
- Every test is catalogued with a one-line description in [TESTS.md](TESTS.md).

## License

This project is licensed under the **BSD 3-Clause License** — see the [LICENSE](LICENSE) file for the full text. Copyright © IDCT — Bartosz Pachołek.
