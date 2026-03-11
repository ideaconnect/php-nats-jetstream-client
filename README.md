# idct/php-nats-jetstream-client

Async-first NATS and JetStream client for PHP 8.2+.

## Status

This project is in active development. Implemented scope includes foundational transport/protocol, subscriptions, request/reply, reconnect with resubscribe recovery, and JetStream account/stream/consumer APIs with publish acknowledgments.

Current scheduling note: scheduled messages are implemented with NATS scheduler headers and currently accept only `@at` expressions.

Use `Idct\\Nats\\JetStream\\Schedule::at(...)` or `Schedule::atTimestamp(...)` to generate valid `@at` expressions.

### Scheduled Publish Example (`@at`)

```php
<?php

declare(strict_types=1);

use Idct\Nats\Connection\NatsOptions;
use Idct\Nats\Core\NatsClient;
use Idct\Nats\JetStream\Schedule;

$client = new NatsClient(new NatsOptions(url: 'nats://127.0.0.1:4222'));
$client->connect();

$jetStream = $client->jetStream();

$jetStream->publishScheduled(
	subject: 'orders.created',
	payload: json_encode(['id' => 123], JSON_THROW_ON_ERROR),
	schedule: Schedule::at(new DateTimeImmutable('+30 seconds')),
);

$client->disconnect();
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
