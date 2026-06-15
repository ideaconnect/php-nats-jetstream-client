<?php

/**
 * Scheduled Publish (@at) - deliver a message at a future time.
 *
 * Creates a schedule-enabled stream and uses publishScheduled() with Schedule::at() to
 * queue a delayed delivery, then polls the target subject (via Direct Get) until the
 * scheduled message lands. Requires allow_msg_schedules (and allow_msg_ttl for a TTL).
 *
 * Mirrors the README "Scheduled Publish Example (@at)" example. Run: php examples/scheduled-publish.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\JetStream\Schedule;

use function Amp\delay;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-scheduled-publish'));
$client->connect()->await();

$stream = 'EX_SCHED';
$js = $client->jetStream();

try {
    // The backing stream must cover the schedule and target subjects and enable scheduling.
    // allow_msg_schedules is required for scheduled publish; allow_msg_ttl is required when
    // you pass scheduleTtl; allow_direct lets this example verify delivery via Direct Get below.
    $js->createStream($stream, [
        'ex.sched.schedules.one',
        'ex.sched.events',
    ], [
        'allow_msg_schedules' => true,
        'allow_msg_ttl' => true,
        'allow_direct' => true,
    ])->await();

    // Schedule a single delayed delivery. Schedule::at() emits the "@at <UTC>" expression.
    $js->publishScheduled(
        scheduleSubject: 'ex.sched.schedules.one',
        targetSubject: 'ex.sched.events',
        payload: json_encode(['id' => 123], JSON_THROW_ON_ERROR),
        schedule: Schedule::at(new DateTimeImmutable('+2 seconds')),
        scheduleTtl: '5m',
    )->await();

    // Poll the target subject (via Direct Get of the last message) until the scheduled
    // delivery lands or a bounded deadline elapses.
    $deadline = hrtime(true) / 1e9 + 8.0;
    $delivered = null;

    while (hrtime(true) / 1e9 < $deadline) {
        try {
            $msg = $js->directGetLastMessageForSubject($stream, 'ex.sched.events')
                ->await(new TimeoutCancellation(2.0));
            $delivered = $msg->payload;
            break;
        } catch (\Throwable) {
            // Not delivered yet; keep polling.
        }

        delay(0.25);
    }

    if ($delivered === null) {
        throw new RuntimeException('scheduled message was not delivered before the deadline');
    }

    echo "OK scheduled-publish: delivered to ex.sched.events with payload {$delivered}\n";
} finally {
    try {
        $js->deleteStream($stream)->await();
    } catch (\Throwable) {
        // best-effort cleanup
    }

    $client->disconnect()->await();
}
