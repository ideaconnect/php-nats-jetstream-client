<?php

/**
 * Services Framework - a NATS micro-service.
 *
 * Builds a service exposing a flat endpoint and a grouped endpoint, calls both, and
 * queries the spec discovery channel ($SRV.PING.<name>). The service shares the
 * connection, so incoming frames are pumped concurrently while requesting.
 *
 * Mirrors the README "Services Framework" example. Run: php examples/services-framework.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;

use function Amp\async;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-services-framework'));
$client->connect()->await();

$service = null;

try {
    // Build a micro-service exposing one flat endpoint plus a grouped (svc.v1.echo) endpoint.
    // Handlers receive the request NatsMessage and return the reply payload (string|array|null).
    $service = $client->service('exframework', '1.0.0', 'Echo demo service')
        ->addEndpoint('echo', 'ex.framework.echo', static function (NatsMessage $message): string {
            return 'reply:' . $message->payload;
        });

    // Grouped endpoint: addGroup() prefixes the subject, so this listens on "ex.framework.v1.echo".
    $service->addGroup('ex.framework')->addGroup('v1')->addEndpoint(
        'echo-v1',
        'echo',
        static function (NatsMessage $message): string {
            return 'v1:' . $message->payload;
        },
    );

    $service->start()->await();
    // Ensure all endpoint + discovery subscriptions are registered server-side before requesting.
    $client->flush()->await();

    // The service runs on this same connection, so pump incoming frames concurrently while we
    // issue requests; otherwise the service handler never runs and request() would time out.
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

    // Call the flat endpoint subject and the grouped endpoint subject.
    $flat = $client->request('ex.framework.echo', 'ping', 2000)->await();
    $grouped = $client->request('ex.framework.v1.echo', 'ping', 2000)->await();

    // Use the spec discovery channel: $SRV.PING.<name> answers with a ping_response envelope.
    $ping = $client->request('$SRV.PING.exframework', '', 2000)->await();
    $pingPayload = json_decode($ping->payload, true, 512, JSON_THROW_ON_ERROR);

    $pump->ignore();

    echo sprintf(
        'OK services-framework: flat=%s grouped=%s ping_type=%s',
        $flat->payload,
        $grouped->payload,
        is_array($pingPayload) ? (string) ($pingPayload['type'] ?? '?') : '?',
    ) . PHP_EOL;
} finally {
    // Best-effort cleanup so re-running is safe: stop the service subscriptions, then disconnect.
    if ($service !== null) {
        try {
            $service->stop()->await();
        } catch (\Throwable) {
            // Ignore cleanup failures.
        }
    }

    $client->disconnect()->await();
}
