# idct/php-nats-jetstream-client

Async-first NATS and JetStream client for PHP 8.2+ with first-class support for core NATS messaging, JetStream, KeyValue, ObjectStore, and NATS microservices.

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
- [Usage](#usage)
- [Authentication Options](#authentication-options)
- [Connect and Publish/Subscribe](#connect-and-publishsubscribe)
- [Request/Reply](#requestreply)
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
- [KeyValue Bucket](#keyvalue-bucket)
- [Object Store Bucket](#object-store-bucket)
- [Object Store Streaming to Callback](#object-store-streaming-to-callback)
- [Services Framework](#services-framework)
- [Services: SCHEMA Discovery](#services-schema-discovery)
- [Graceful Drain](#graceful-drain)
- [Ordered Consumer](#ordered-consumer)
- [Consumer Pause/Resume](#consumer-pauseresume)
- [Fetch Batch](#fetch-batch)
- [Stream Purge and List](#stream-purge-and-list)
- [Consumer List](#consumer-list)
- [Stream Message Direct Get](#stream-message-direct-get)
- [Credentials File Authentication](#credentials-file-authentication)
- [Typed Stream Configuration](#typed-stream-configuration)
- [Pull Consumer Batching/Iteration](#pull-consumer-batchingiteration)
- [Stream Mirroring and Sourcing](#stream-mirroring-and-sourcing)
- [Republish and Subject Transform](#republish-and-subject-transform)
- [Compatibility Mapping](#compatibility-mapping)
- [Behavior Notes](#behavior-notes)
- [Configuration Option Mapping](#configuration-option-mapping)
- [Performance Benchmark Recipe](#performance-benchmark-recipe)
- [Testing](#testing)
- [Contributing](#contributing)
- [Current Test Baseline](#current-test-baseline)

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
- JetStream direct message get from stream
- Scheduled publish (`@at` support)
- KeyValue API (bucket lifecycle with history/TTL/storage options, put/get/update/delete/purge, watch, getAll/status)
- ObjectStore API (bucket lifecycle, put/get/delete/list/watch, chunked uploads, SHA-256 digest verification)
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

Current scheduling note: scheduled messages are implemented with NATS scheduler headers and currently accept only `@at` expressions.

Use `IDCT\\NATS\\JetStream\\Schedule::at(...)` or `Schedule::atTimestamp(...)` to generate valid `@at` expressions.

## Usage

### Authentication Options

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

### Connect and Publish/Subscribe

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

### Headers and Server Info

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

### JetStream Pull Consumer (Fetch + ACK)

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

### JetStream Pull Consumer (NAK, Delayed NAK, TERM, In-Progress)

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

// Blocking fetch — waits up to the configured timeout.
$msg = $queue->next();

// Batch fetch — collects up to 10 messages within the timeout window.
$messages = $queue->fetchAll(limit: 10);

$client->unsubscribe($queue->sid)->await();
$client->disconnect()->await();
```

### JetStream Push Consumer (Durable)

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

$jetStream->publishScheduled(
	scheduleSubject: 'schedules.orders.one',
	targetSubject: 'events.orders',
	payload: json_encode(['id' => 123], JSON_THROW_ON_ERROR),
	schedule: Schedule::at(new DateTimeImmutable('+30 seconds')),
	scheduleTtl: '5m',
)->await();

$client->disconnect()->await();
```

### KeyValue Bucket

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\JetStream\KeyValue\KeyValueEntry;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$kv = $client->jetStream()->keyValue('cfg');
$kv->create()->await();

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

$watchSid = $kv->watch(static function (KeyValueEntry $entry): void {
	echo $entry->key . ':' . ($entry->value ?? '<deleted>') . PHP_EOL;
}, 'theme')->await();

$kv->delete('theme')->await();
$kv->purge('theme')->await();
$client->processIncoming()->await();

$client->unsubscribe($watchSid)->await();
$kv->deleteBucket()->await();
$client->disconnect()->await();
```

### Object Store Bucket

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

### Object Store Streaming to Callback

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

$info = $store->getToCallback('logo.txt', static function (string $chunk): void {
	echo $chunk;
})->await();

echo PHP_EOL;
echo $info?->name . PHP_EOL;

$store->deleteBucket()->await();
$client->disconnect()->await();
```

### Services Framework

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

### Services: SCHEMA Discovery

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

### Stream Message Direct Get

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

### Credentials File Authentication

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

### Stream Mirroring and Sourcing

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
| Object Store | extended | Bucket/object lifecycle, object listing, chunked uploads, and digest verification are covered. |

## Behavior Notes

### `processIncoming()`

`processIncoming()` reads a single transport chunk, parses all complete frames from it, and dispatches them to subscription callbacks. It is **non-blocking** — if no data is available, it returns immediately with a frame count of `0`. Call it in a loop to process multiple messages:

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

### Reconnect Behavior

When a connection drops and `reconnectEnabled` is `true`:

1. **Exponential backoff**: delay is computed as `reconnectDelayMs * 2^(attempt - 1)`, capped at `reconnectMaxDelayMs`, with random jitter up to `reconnectJitterMs`.
2. **Server rotation**: the client cycles through configured servers in order.
3. **Subscription replay**: all active subscriptions are replayed (SUB commands resent) after reconnect.
4. **Replay validation**: reconnect does not treat replayed subscriptions as successful if the server immediately answers with a fatal `-ERR` during replay. In that case reconnect keeps retrying until a healthy server accepts the replay or attempts are exhausted.
5. **Published messages during reconnect are lost**: there is no outbound buffer for in-flight publishes. Only subscriptions are restored.

Recoverable server `-ERR` frames such as `Invalid Subject` or `Permissions Violation for Publish/Subscription to ...` do not automatically close an already-open connection. Fatal connection-level errors still do.

The initial handshake is bounded by `connectTimeoutMs`, not by a fixed number of transport reads. During bootstrap the client will also answer server `PING` frames and process async `INFO` updates while waiting for the initial `PONG`.

### Ordered Consumer Gap Recovery

`subscribeOrderedConsumer()` automatically detects stream sequence gaps in delivered messages. When a gap is detected, the consumer is transparently deleted and recreated starting from the expected sequence, and the current message is still forwarded to the user callback before sequence tracking resumes.

## Configuration Option Mapping

`NatsOptions` fields and defaults:

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
| `tlsHandshakeFirst` | `bool` | `false` | Performs TLS handshake immediately after connecting, before server INFO. |
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

## Performance Benchmark Recipe

Quick local publish/request benchmark (single process):

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;

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

$start = hrtime(true);
for ($i = 0; $i < $iterations; $i++) {
	$loop = Amp\async(static function () use ($server): void {
		$server->processIncoming()->await();
	});
	$client->request($subject, 'x', 2000)->await();
	$loop->await();
}
$elapsedNs = hrtime(true) - $start;

$totalMs = $elapsedNs / 1_000_000;
$rps = $iterations / max(0.001, ($elapsedNs / 1_000_000_000));

echo 'iterations=' . $iterations . PHP_EOL;
echo 'total_ms=' . number_format($totalMs, 2, '.', '') . PHP_EOL;
echo 'req_per_sec=' . number_format($rps, 2, '.', '') . PHP_EOL;

$client->disconnect()->await();
$server->disconnect()->await();
```

Run recipe:

```bash
docker compose up -d
php -d zend.assertions=1 path/to/benchmark.php
docker compose down
```

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
```

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

## Contributing

Contributions should keep changes focused and paired with the narrowest useful verification.

- Add or update tests when behavior changes.
- Prefer `composer test:unit` for small local changes and `composer test:e2e` for auth, transport, protocol, JetStream, or integration-fixture changes.
- Do not hand-edit generated JWT fixture files under `build/nats/jwt/`; regenerate them with `composer fixture:jwt`.
- Run `composer stan` for code changes and `composer fix` when style adjustments are needed.
- Review `AGENTS.md` for repository structure, standards, and continuation guidance before larger changes.

## Current Test Baseline

- Unit tests cover protocol encoding/parsing, handshake/state transitions, subscriptions, backpressure policies, request/reply flows, reconnect/server-rotation behavior, and exponential backoff.
- Unit tests also cover JetStream account info, stream and consumer CRUD, publish acknowledgments, API error mapping, fetch batch, ordered consumers, consumer pause/resume, KV bucket options, ObjectStore chunking and digest verification.
- Unit tests cover microservices framework including PING/INFO/STATS/SCHEMA discovery and grouped endpoint hierarchy.
- Integration tests cover live connect/disconnect, publish-subscribe roundtrip, request-reply, connection rotation fallback, JetStream stream/consumer lifecycle with publish-ack flow, KV operations, ObjectStore operations, and service discovery.
- Integration tests also cover local token auth, username/password auth, TLS handshake-first auth including strict peer-validation, hostname mismatch, and missing-client-cert failures, resolver-backed JWT auth, and standalone NKey auth.
- Static analysis runs with PHPStan level 8.
