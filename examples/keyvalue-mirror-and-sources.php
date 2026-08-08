<?php

/**
 * KeyValue Mirror and Sources - replicate a KV bucket, and attach to it with bind().
 *
 * Creates an origin bucket, a SOURCING bucket that copies the origin's entries into its own
 * subject prefix, and a MIRROR bucket. A second handle to the mirror bucket - one that never ran
 * create() - calls bind() to pick up the mirror prefixes from the server, and then reads and writes
 * through the mirror.
 *
 * Run: php examples/keyvalue-mirror-and-sources.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;

use function Amp\delay;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-keyvalue-mirror-and-sources'));
$client->connect()->await();

$origin = 'EX_KV_ORIGIN';
$sourcing = 'EX_KV_SOURCING';
$mirror = 'EX_KV_MIRROR';

$js = $client->jetStream();
$originKv = $js->keyValue($origin);
$sourcingKv = $js->keyValue($sourcing);
$mirrorKv = $js->keyValue($mirror);

// Replication (sourcing and mirroring) runs in the background on the server, so every read of a
// replicated entry is polled until it lands or this bound elapses.
$waitSeconds = 5.0;

try {
    $originKv->create()->await();
    $originKv->put('greeting', 'hello-from-origin')->await();

    // A SOURCING bucket keeps its own subjects and copies records in from other buckets. Name the
    // origin with the "bucket" key and the client resolves its backing KV_ stream and attaches the
    // KV subject transform, which re-subjects each copied record from $KV.<origin>.<key> to
    // $KV.<sourcing>.<key>. Without that transform the copies would sit in the stream under the
    // origin's subjects, where this bucket's own reads and watches never look.
    $sourcingKv->create(['sources' => [['bucket' => $origin]]])->await();

    // The sourced entry is read through the sourcing bucket's own handle - no bind() needed, a
    // sourcing bucket uses its own prefix for both reads and writes.
    $sourcedValue = null;
    $deadline = hrtime(true) / 1e9 + $waitSeconds;
    while ($sourcedValue === null && hrtime(true) / 1e9 < $deadline) {
        $sourcedValue = $sourcingKv->get('greeting')->await()?->value;
        if ($sourcedValue === null) {
            delay(0.05);
        }
    }

    if ($sourcedValue === null) {
        throw new RuntimeException('the sourced entry did not become visible before the deadline');
    }

    // A MIRROR bucket is different: it defines no subjects of its own, it only replicates the origin
    // stream. Two consequences the client handles for you:
    //  - writes have to go through to the origin bucket's subjects, because nothing ingests
    //    $KV.<mirror>.<key>;
    //  - mirrored records keep the origin's subjects unless a subject transform re-subjects them,
    //    so the transform below is what makes reads through this bucket's own prefix work.
    $mirrorKv->create(['mirror' => [
        'bucket' => $origin,
        'subject_transforms' => [[
            'src' => '$KV.' . $origin . '.>',
            'dest' => '$KV.' . $mirror . '.>',
        ]],
    ]])->await();

    // A SECOND handle to the same mirror bucket. keyValue() only takes a bucket name: this handle
    // never saw the mirror configuration, so it still assumes writes go to $KV.<mirror>.<key>. That
    // subject is bound to no stream, so the write fails with a 503 "no JetStream responder".
    $secondHandle = $js->keyValue($mirror);
    $unboundWriteCode = null;
    try {
        $secondHandle->put('unbound', 'this write has nowhere to go')->await();
    } catch (JetStreamException $e) {
        $unboundWriteCode = $e->getCode();
    }

    if ($unboundWriteCode !== 503) {
        throw new RuntimeException('expected a write from the unbound mirror handle to fail with 503');
    }

    // bind() fetches the bucket's server-side stream configuration and resolves the mirror prefixes
    // from it, which is exactly what create() did for the handle that created the bucket. It returns
    // the same handle, so $js->keyValue($mirror)->bind()->await() works as a one-liner too.
    $secondHandle->bind()->await();

    // Reads through the bound handle now use this bucket's prefix, where the subject transform put
    // the replicated records.
    $mirroredValue = null;
    $deadline = hrtime(true) / 1e9 + $waitSeconds;
    while ($mirroredValue === null && hrtime(true) / 1e9 < $deadline) {
        $mirroredValue = $secondHandle->get('greeting')->await()?->value;
        if ($mirroredValue === null) {
            delay(0.05);
        }
    }

    if ($mirroredValue === null) {
        throw new RuntimeException('the mirrored entry did not become visible before the deadline');
    }

    // Writes through the bound handle publish to the ORIGIN bucket's subject, so the ack names the
    // origin's stream (KV_<origin>) and the record replicates back into the mirror.
    $ack = $secondHandle->put('mirror-write', 'hello-from-mirror-handle')->await();

    $readBack = null;
    $deadline = hrtime(true) / 1e9 + $waitSeconds;
    while ($readBack === null && hrtime(true) / 1e9 < $deadline) {
        $readBack = $secondHandle->get('mirror-write')->await()?->value;
        if ($readBack === null) {
            delay(0.05);
        }
    }

    if ($readBack === null) {
        throw new RuntimeException('the write through the mirror handle did not replicate back before the deadline');
    }

    echo 'OK keyvalue-mirror-and-sources: sourced "' . $sourcedValue . '", mirror read "' . $mirroredValue
        . '", mirror write acked by ' . $ack->stream . ' and read back as "' . $readBack . "\"\n";
} finally {
    // Delete the replicas before the origin they follow.
    foreach ([$mirrorKv, $sourcingKv, $originKv] as $bucket) {
        try {
            $bucket->deleteBucket()->await();
        } catch (\Throwable) {
            // best-effort cleanup
        }
    }

    $client->disconnect()->await();
}
