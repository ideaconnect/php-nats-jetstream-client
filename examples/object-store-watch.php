<?php

/**
 * Object Store watch - observe object metadata changes as they happen.
 *
 * Demonstrates the full watch contract: a watch that replays the current metadata of the
 * objects already in the bucket and then follows live updates, ObjectStoreWatchOptions with an
 * explicit idle heartbeat, the exactName parameter for an object whose NAME contains a subject
 * wildcard character, and stopping a watch with JetStreamContext::stopOrderedConsumer().
 *
 * Mirrors the README "Object Store watch" example. Run: php examples/object-store-watch.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Amp\CancelledException;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\ObjectStore\ObjectInfo;
use IDCT\NATS\JetStream\ObjectStore\ObjectStoreWatchOptions;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-object-store-watch'));
$client->connect()->await();

$bucket = 'EX_OBJ_WATCH';
$js = $client->jetStream();
$store = $js->objectStore($bucket);

// Drives frame delivery until the predicate holds or the deadline passes, so no wait is unbounded.
$pumpUntil = static function (callable $satisfied, float $seconds) use ($client): bool {
    $deadline = hrtime(true) + (int) ($seconds * 1e9);
    while (!$satisfied() && hrtime(true) < $deadline) {
        try {
            $client->processIncoming(new TimeoutCancellation(0.25))->await();
        } catch (CancelledException) {
            // No frame in this window; keep polling until the deadline.
        }
    }

    return $satisfied();
};

try {
    $store->create()->await();

    // This object exists BEFORE the watch starts, so the snapshot phase below replays it.
    $store->put('release-notes.txt', 'v1 notes')->await();

    // Passing ObjectStoreWatchOptions selects the reference "snapshot then follow" behavior: the
    // current metadata of every existing object first, then live updates. The explicit idle
    // heartbeat (nanoseconds) is what the watch consumer asks the server for; it arms the
    // missed-heartbeat watchdog that reports a silent or reaped consumer instead of hanging.
    // Leaving it null keeps the bucket default of 5 seconds.
    $allOptions = new ObjectStoreWatchOptions(idleHeartbeat: 2_000_000_000);

    /** @var array<string,int> $seen */
    $seen = [];
    $allSid = $store->watch(
        static function (ObjectInfo $info) use (&$seen): void {
            // Each update carries the object metadata; revision is the stream sequence of the record.
            $seen[$info->name] = $info->revision ?? 0;
        },
        '>',
        $allOptions,
    )->await();

    // Make sure the consumer and its subscription are live before publishing the live update.
    $client->flush()->await();

    $store->put('logo.txt', 'hello-object')->await();

    $sawBoth = $pumpUntil(static function () use (&$seen): bool {
        return isset($seen['release-notes.txt'], $seen['logo.txt']);
    }, 5.0);

    if (!$sawBoth) {
        throw new RuntimeException(
            'watch did not deliver the replayed and the live object within the deadline, saw: '
            . implode(',', array_keys($seen)),
        );
    }

    // A watch rides an ordered consumer, so stop it through the context. A recreate rotates the
    // internal sid, which makes the sid watch() returned stale for a plain $client->unsubscribe();
    // the context resolves the current sid and deletes the server-side consumer as well.
    $js->stopOrderedConsumer($allSid)->await();

    // An object name is free-form, so it may contain "*" or ">". Meta subjects carry base64url
    // encoded names, so such a name is not a subject wildcard and a plain pattern cannot express
    // it - watch() rejects the ambiguous case rather than subscribing to something that would
    // silently observe nothing.
    $wildcardName = 'report>2024';

    try {
        $store->watch(static function (ObjectInfo $info): void {}, $wildcardName)->await();

        throw new RuntimeException('watch() accepted a wildcard pattern without exactName');
    } catch (JetStreamException) {
        // Expected: the pattern looks like a subject wildcard.
    }

    // exactName: true declares the pattern to be a NAME, so it is encoded as one and the watch
    // follows exactly that object - the only way to watch a name containing "*" or ">".
    /** @var list<string> $exactSeen */
    $exactSeen = [];
    $exactSid = $store->watch(
        static function (ObjectInfo $info) use (&$exactSeen): void {
            $exactSeen[] = $info->name . ' (rev ' . ($info->revision ?? 0) . ')';
        },
        $wildcardName,
        // updatesOnly skips the snapshot phase: only objects changed after this point are delivered.
        new ObjectStoreWatchOptions(updatesOnly: true, idleHeartbeat: 2_000_000_000),
        exactName: true,
    )->await();

    $client->flush()->await();

    // Written first, so a watch that did NOT filter on the exact name would have to deliver this
    // object before the one below - the metadata records reach the watcher in stream order.
    $store->put('release-notes.txt', 'v2 notes')->await();
    $store->put($wildcardName, 'quarterly figures')->await();

    $sawExact = $pumpUntil(static function () use (&$exactSeen): bool {
        return $exactSeen !== [];
    }, 5.0);

    if (!$sawExact) {
        throw new RuntimeException('exactName watch did not deliver "' . $wildcardName . '" within the deadline');
    }

    if (count($exactSeen) !== 1) {
        throw new RuntimeException(
            'exactName watch delivered unrelated objects: ' . implode(', ', $exactSeen),
        );
    }

    $js->stopOrderedConsumer($exactSid)->await();

    echo 'OK object-store-watch: watcher saw [' . implode(',', array_keys($seen))
        . '], exact-name watcher saw ' . $exactSeen[0]
        . ', idle heartbeat 2s' . PHP_EOL;
} finally {
    try {
        $store->deleteBucket()->await();
    } catch (\Throwable) {
        // best-effort cleanup
    }

    $client->disconnect()->await();
}
