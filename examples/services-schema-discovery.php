<?php

/**
 * Services: SCHEMA Discovery - request validation + observability.
 *
 * An endpoint declares a JSON schema validated by BasicJsonSchemaValidator (invalid
 * requests get a VALIDATION_ERROR reply), registers a lifecycle observer, and exposes
 * its schema via the $SRV.SCHEMA.<name> discovery channel.
 *
 * Mirrors the README "Services: SCHEMA Discovery" example. Run: php examples/services-schema-discovery.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Services\BasicJsonSchemaValidator;

use function Amp\async;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-services-schema-discovery'));
$client->connect()->await();

$service = null;

try {
    // Capture lifecycle events so we can show the observer firing (request_start/request_end/...).
    $events = [];

    // Build a "calc" service whose "add" endpoint declares a JSON schema and validates requests
    // with the built-in BasicJsonSchemaValidator (rejected payloads get a VALIDATION_ERROR reply).
    $service = $client->service('excalc', '1.0.0', 'Calculator')
        ->withSchemaValidator(new BasicJsonSchemaValidator())
        ->addObserver(static function (string $event, $endpoint, NatsMessage $message, array $context) use (&$events): void {
            // Example events: request_start, request_error, request_end.
            $events[] = $event;
        })
        ->addEndpoint('add', 'ex.calc.add', static function (NatsMessage $message): string {
            $request = json_decode($message->payload, true, 512, JSON_THROW_ON_ERROR);
            $sum = (int) ($request['a'] ?? 0) + (int) ($request['b'] ?? 0);

            return json_encode(['result' => $sum], JSON_THROW_ON_ERROR);
        }, schema: [
            'type' => 'object',
            'required' => ['a', 'b'],
            'properties' => [
                'a' => ['type' => 'integer'],
                'b' => ['type' => 'integer'],
            ],
        ]);

    $service->start()->await();
    $client->flush()->await();

    // The service shares this connection, so pump incoming frames concurrently while we request.
    $pump = async(function () use ($client): void {
        $deadline = hrtime(true) / 1e9 + 6.0;
        while (hrtime(true) / 1e9 < $deadline) {
            try {
                $client->processIncoming(new TimeoutCancellation(0.2))->await();
            } catch (\Amp\CancelledException) {
                // No frame this cycle; keep pumping until the deadline.
            }
        }
    });

    // Discover the endpoint schema via the $SRV.SCHEMA.<name> channel.
    $schemaReply = $client->request('$SRV.SCHEMA.excalc', '', 2000)->await();
    $schemaResponse = json_decode($schemaReply->payload, true, 512, JSON_THROW_ON_ERROR);
    $discoveredSchema = $schemaResponse['endpoints'][0]['schema'] ?? null;

    // A valid request succeeds.
    $valid = $client->request('ex.calc.add', json_encode(['a' => 2, 'b' => 3], JSON_THROW_ON_ERROR), 2000)->await();
    $validResult = json_decode($valid->payload, true, 512, JSON_THROW_ON_ERROR);

    // An invalid request (missing required "b") is rejected with a structured VALIDATION_ERROR envelope.
    $invalid = $client->request('ex.calc.add', json_encode(['a' => 1], JSON_THROW_ON_ERROR), 2000)->await();
    $invalidResponse = json_decode($invalid->payload, true, 512, JSON_THROW_ON_ERROR);

    $pump->ignore();

    echo sprintf(
        'OK services-schema-discovery: schema_required=%s valid_result=%s rejected_code=%s observers=%d',
        is_array($discoveredSchema) ? implode(',', $discoveredSchema['required'] ?? []) : '?',
        (string) ($validResult['result'] ?? '?'),
        is_array($invalidResponse) ? (string) ($invalidResponse['code'] ?? '?') : '?',
        count($events),
    ) . PHP_EOL;
} finally {
    // Best-effort cleanup so re-running is safe.
    if ($service !== null) {
        try {
            $service->stop()->await();
        } catch (\Throwable) {
            // Ignore cleanup failures.
        }
    }

    $client->disconnect()->await();
}
