<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Idct\Nats\Connection\NatsOptions;
use Idct\Nats\Core\NatsClient;

$options = new NatsOptions(servers: ['nats://127.0.0.1:4222'], name: 'example-client');
$client = new NatsClient($options);

$client->connect()->await();
$client->publish('example.subject', json_encode(['hello' => 'world'], JSON_THROW_ON_ERROR))->await();
$client->disconnect()->await();

echo "Published message\n";
