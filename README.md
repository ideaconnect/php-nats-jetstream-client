# idct/php-nats-jetstream-client

Async-first NATS and JetStream client for PHP 8.2+.

## Status

This project is in active development.

Implemented functionality includes:

- Core NATS connect/disconnect
- Publish and subscribe
- Request/reply with timeout and cancellation
- Reconnect with server rotation and subscription replay
- JetStream account info
- JetStream stream CRUD
- JetStream consumer CRUD (durable + ephemeral)
- JetStream pull consumers (fetch next, ACK/NAK/TERM/WPI, delayed NAK)
- JetStream push consumers with heartbeat/flow-control handling
- JetStream publish ACK
- Scheduled publish (`@at` support)
- KeyValue API slice (bucket lifecycle, put/get/delete, watch)
- Server authorization methods: token, username/password, JWT + nonce signer

Current scheduling note: scheduled messages are implemented with NATS scheduler headers and currently accept only `@at` expressions.

Use `Idct\\Nats\\JetStream\\Schedule::at(...)` or `Schedule::atTimestamp(...)` to generate valid `@at` expressions.

## Usage

### Authentication Options

```php
<?php

declare(strict_types=1);

use Idct\Nats\Connection\NatsOptions;
use Idct\Nats\Core\NatsClient;
use Idct\Nats\Auth\NonceSignerInterface;

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
```

### Connect and Publish/Subscribe

```php
<?php

declare(strict_types=1);

use Idct\Nats\Connection\NatsOptions;
use Idct\Nats\Core\NatsClient;
use Idct\Nats\Core\NatsMessage;

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

use Idct\Nats\Connection\NatsOptions;
use Idct\Nats\Core\NatsClient;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$reply = $client->request('svc.echo', '{"hello":"world"}', 2000)->await();
echo $reply->payload . PHP_EOL;

$client->disconnect()->await();
```

### JetStream Stream and Durable Consumer

```php
<?php

declare(strict_types=1);

use Idct\Nats\Connection\NatsOptions;
use Idct\Nats\Core\NatsClient;

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

### JetStream Pull Consumer (Fetch + ACK)

```php
<?php

declare(strict_types=1);

use Idct\Nats\Connection\NatsOptions;
use Idct\Nats\Core\NatsClient;

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

### JetStream Push Consumer (Durable)

```php
<?php

declare(strict_types=1);

use Idct\Nats\Connection\NatsOptions;
use Idct\Nats\Core\NatsClient;
use Idct\Nats\Core\NatsMessage;

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

use Idct\Nats\Connection\NatsOptions;
use Idct\Nats\Core\NatsClient;
use Idct\Nats\Core\NatsMessage;

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

use Idct\Nats\Connection\NatsOptions;
use Idct\Nats\Core\NatsClient;
use Idct\Nats\JetStream\Schedule;
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

use Idct\Nats\Connection\NatsOptions;
use Idct\Nats\Core\NatsClient;
use Idct\Nats\JetStream\KeyValueEntry;

$client = new NatsClient(new NatsOptions());
$client->connect()->await();

$kv = $client->jetStream()->keyValue('cfg');
$kv->create()->await();

$kv->put('theme', 'dark')->await();
$entry = $kv->get('theme')->await();
echo $entry?->value . PHP_EOL;

$watchSid = $kv->watch(static function (KeyValueEntry $entry): void {
	echo $entry->key . ':' . ($entry->value ?? '<deleted>') . PHP_EOL;
}, 'theme')->await();

$kv->delete('theme')->await();
$client->processIncoming()->await();

$client->unsubscribe($watchSid)->await();
$kv->deleteBucket()->await();
$client->disconnect()->await();
```

### Object Store Bucket

```php
<?php

declare(strict_types=1);

use Idct\Nats\Connection\NatsOptions;
use Idct\Nats\Core\NatsClient;

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

$store->delete('logo.txt')->await();
$store->deleteBucket()->await();
$client->disconnect()->await();
```

### Services Framework

```php
<?php

declare(strict_types=1);

use Idct\Nats\Connection\NatsOptions;
use Idct\Nats\Core\NatsClient;
use Idct\Nats\Core\NatsMessage;

$serviceClient = new NatsClient(new NatsOptions());
$serviceClient->connect()->await();

$service = $serviceClient->service('echo', '1.0.0', 'Echo demo')
	->addEndpoint('echo', 'svc.echo', static function (NatsMessage $message): string {
		return 'reply:' . $message->payload;
	});

$service->start()->await();

// In another client you can call discovery or endpoint subjects:
// - $SRV.PING.echo
// - $SRV.INFO.echo
// - $SRV.STATS.echo
// - svc.echo

$service->stop()->await();
$serviceClient->disconnect()->await();
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

- Unit tests cover protocol encoding/parsing, handshake/state transitions, subscriptions, backpressure policies, request/reply flows, and reconnect/server-rotation behavior.
- Unit tests also cover JetStream account info, stream and consumer CRUD, publish acknowledgments, and API error mapping.
- Integration tests cover live connect/disconnect, publish-subscribe roundtrip, request-reply, connection rotation fallback, and JetStream stream/consumer lifecycle with publish-ack flow.
- Static analysis runs with PHPStan level 8.

## Roadmap

See the implementation checklist in docs/NEXT_STEPS.md.
