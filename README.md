# idct/php-nats-jetstream-client

Async-first NATS and JetStream client for PHP 8.2+.

## Status

This project is in active development.

Implemented functionality includes:

- Core NATS connect/disconnect with graceful drain
- Publish and subscribe
- Request/reply with timeout and cancellation
- Reconnect with exponential backoff, server rotation, and subscription replay
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
- Server authorization methods: token, username/password, JWT + nonce signer, credentials file parser
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

### Compatibility Mapping (basis-company README)

This repository tracks parity against basis-company nats.php README examples.

| Section | Status | Notes |
| --- | --- | --- |
| Connecting and Auth | workflow parity | Basic, token, username/password, JWT nonce signing, credentials file, and TLS CA/cert/key options are supported. |
| Publish Subscribe | workflow parity | Callback, queue-group, and polling queue (`SubscriptionQueue` with `fetch()`/`next()`/`fetchAll()`) patterns are supported. |
| Request Response | workflow parity | Awaited request/reply with timeout and cancellation is covered, but the API shape differs from basis-company's `dispatch()` and callback request helpers. |
| JetStream API Usage | workflow parity | Stream/consumer lifecycle, pull/push flows, ephemeral consumers, scheduling, ordered-consumer helpers, batching/iteration chain API, stream mirroring/sourcing, republish/subject-transform helpers, and typed enums are covered. |
| Microservices | partial parity | Service registration, discovery, endpoint handling, grouped hierarchy, and SCHEMA discovery are covered with a slimmer stats/handler model. |
| Key Value Storage | workflow parity | Core KV flows plus update/purge/getAll/status parity are covered. |
| Object Store | extended | Bucket/object lifecycle, object listing, chunked uploads, and digest verification are covered. |

Detailed execution plan: see REGRESSION_PASS.md.

### Authentication Options

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Auth\NonceSignerInterface;

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

// JWT + nonce signature.
final class DemoNonceSigner implements NonceSignerInterface
{
	public function sign(string $nonce): string
	{
		// Replace with real JWT nonce signing implementation.
		return base64_encode(hash('sha256', $nonce, true));
	}
}

$jwtClient = new NatsClient(new NatsOptions(
	servers: ['nats://127.0.0.1:4222'],
	jwt: 'your-jwt-token',
	nkey: 'U...PUBLIC NKEY...',
	nonceSigner: new DemoNonceSigner(),
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
);

$client->disconnect()->await();
```

### KeyValue Bucket

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\JetStream\KeyValueEntry;

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
```

### Services: SCHEMA Discovery

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$service = $client->service('calc', '1.0.0', 'Calculator')
	->addEndpoint('add', 'calc.add', static function (NatsMessage $message): string {
		return 'result';
	}, schema: ['request' => 'int a, int b', 'response' => 'int sum']);

$service->start()->await();

// Another client can discover the schema:
// $reply = $client->request('$SRV.SCHEMA.calc', '')->await();
// The response includes endpoint schemas in the JSON payload.

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
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

// Parse a .creds file to extract JWT and NKey seed.
$creds = CredentialsParser::fromFile('/path/to/user.creds');

$client = new NatsClient(new NatsOptions(
	servers: ['nats://127.0.0.1:4222'],
	jwt: $creds['jwt'],
	nkey: $creds['nkeySeed'],
	// Provide a nonce signer that uses the NKey seed for Ed25519 signing.
	nonceSigner: new YourNonceSigner($creds['nkeySeed']),
));
```

### Typed Stream Configuration

Stream and consumer configuration supports typed enums for type-safe options:

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\JetStream\RetentionPolicy;
use IDCT\NATS\JetStream\StorageBackend;
use IDCT\NATS\JetStream\DiscardPolicy;
use IDCT\NATS\JetStream\DeliverPolicy;
use IDCT\NATS\JetStream\AckPolicy;
use IDCT\NATS\JetStream\ReplayPolicy;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$js = $client->jetStream();

// Create stream with typed configuration.
$js->createStream('ORDERS', ['orders.>'], [
	'retention' => RetentionPolicy::WorkQueue->value,
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

Use `StreamSource` to configure stream mirrors and aggregated sources:

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\JetStream\StreamSource;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$js = $client->jetStream();

// Create a mirror of an origin stream.
$js->createStream('ORDERS_MIRROR', [], [
	'mirror' => StreamSource::mirror('ORDERS')->toArray(),
])->await();

// Create an aggregate stream from multiple sources.
$js->createStream('ALL_EVENTS', ['events.>'], [
	'sources' => [
		StreamSource::source('ORDERS')->filterSubject('orders.>')->toArray(),
		StreamSource::source('PAYMENTS')->startSeq(100)->toArray(),
	],
])->await();

// Cross-cluster source with external API reference.
$js->createStream('REMOTE_MIRROR', [], [
	'mirror' => StreamSource::mirror('ORIGIN')
		->external('$JS.hub.API', '_DELIVER.hub')
		->toArray(),
])->await();

$client->disconnect()->await();
```

### Republish and Subject Transform

Configure republish rules and subject transforms on streams:

```php
<?php

declare(strict_types=1);

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\JetStream\Republish;
use IDCT\NATS\JetStream\SubjectTransform;

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

### Reconnect Behavior

When a connection drops and `reconnectEnabled` is `true`:

1. **Exponential backoff**: delay is computed as `reconnectDelayMs * 2^(attempt - 1)`, capped at `reconnectMaxDelayMs`, with random jitter up to `reconnectJitterMs`.
2. **Server rotation**: the client cycles through configured servers in order.
3. **Subscription replay**: all active subscriptions are replayed (SUB commands resent) after reconnect.
4. **Published messages during reconnect are lost**: there is no outbound buffer for in-flight publishes. Only subscriptions are restored.

### Ordered Consumer Gap Recovery

`subscribeOrderedConsumer()` automatically detects stream sequence gaps in delivered messages. When a gap is detected, the consumer is transparently deleted and recreated starting from the expected sequence. The user callback is not invoked for gap messages.

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
| `nkey` | `?string` | `null` | Public NKey for JWT auth mode. |
| `nonceSigner` | `?NonceSignerInterface` | `null` | Signs server nonce for JWT mode. |
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

## Development

```bash
composer install
composer test
composer stan
```

## Integration Tests

```bash
docker compose up -d
RUN_INTEGRATION=1 composer test:integration
docker compose down
```

Optional environment variable:

- `NATS_URL` (default: `nats://127.0.0.1:14222`)

## Current Test Baseline

- Unit tests cover protocol encoding/parsing, handshake/state transitions, subscriptions, backpressure policies, request/reply flows, reconnect/server-rotation behavior, and exponential backoff.
- Unit tests also cover JetStream account info, stream and consumer CRUD, publish acknowledgments, API error mapping, fetch batch, ordered consumers, consumer pause/resume, KV bucket options, ObjectStore chunking and digest verification.
- Unit tests cover microservices framework including PING/INFO/STATS/SCHEMA discovery and grouped endpoint hierarchy.
- Integration tests cover live connect/disconnect, publish-subscribe roundtrip, request-reply, connection rotation fallback, JetStream stream/consumer lifecycle with publish-ack flow, KV operations, ObjectStore operations, and service discovery.
- Static analysis runs with PHPStan level 8.

## Roadmap

See the implementation checklist in docs/NEXT_STEPS.md.
