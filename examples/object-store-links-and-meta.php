<?php

/**
 * Object Store Links & Metadata - links, renames without re-upload, and sealing.
 *
 * Demonstrates updateMeta() (rename and metadata replacement that keep the stored
 * chunks, referenced by NUID), addLink() / addBucketLink() (link objects that store no
 * content), the addLink guard that refuses a name already held by a stored object or by
 * its deleted tombstone, and seal() on a throwaway bucket (sealing is irreversible).
 *
 * Mirrors the README "Object Store" links/metadata snippet. Run: php examples/object-store-links-and-meta.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-object-store-links-and-meta'));
$client->connect()->await();

$bucket = 'EX_OBJ_LINKS';
// A second, throwaway bucket: seal() is irreversible, so it must not be the bucket we still write to.
$sealedBucket = 'EX_OBJ_LINKS_SEALED';

$js = $client->jetStream();
$store = $js->objectStore($bucket);
$sealedStore = $js->objectStore($sealedBucket);

$payload = "quarterly numbers\n";
$rejectedLinkNames = [];

try {
    $store->create()->await();
    $sealedStore->create()->await();

    // Store the object we will rename, link to, and re-tag below.
    $stored = $store->put('report.txt', $payload, ['team' => 'analytics', 'content-type' => 'text/plain'])->await();

    // updateMeta() with a new name renames the object WITHOUT re-uploading its bytes: the meta record
    // keeps the same NUID, and the chunks live under $O.<bucket>.C.<nuid>, so nothing is copied.
    $renamed = $store->updateMeta('report.txt', 'report-2026.txt')->await();

    if ($renamed->nuid !== $stored->nuid || $renamed->chunks !== $stored->chunks || $renamed->digest !== $stored->digest) {
        throw new RuntimeException('rename did not preserve the stored chunks (NUID/chunks/digest changed)');
    }

    // Proof the bytes survived the rename untouched: read them back under the new name. get() verifies
    // the SHA-256 digest from the metadata while reassembling, so a mismatch would throw here.
    $afterRename = $store->get('report-2026.txt')->await();
    if ($afterRename === null || $afterRename->data !== $payload) {
        throw new RuntimeException('object bytes are not readable under the new name after the rename');
    }

    // The old name is left as a deleted tombstone: it no longer resolves to content, but the record
    // stays visible through info(). That matters for the addLink guard demonstrated below.
    $oldName = $store->info('report.txt')->await();
    if ($oldName === null || !$oldName->deleted) {
        throw new RuntimeException('the old name should be a deleted tombstone after the rename');
    }

    // updateMeta() with $newName = null keeps the name and REPLACES the whole metadata bag (it is not
    // merged), again without touching the stored bytes.
    $retagged = $store->updateMeta('report-2026.txt', null, ['team' => 'brand'])->await();
    if ($retagged->nuid !== $stored->nuid || $retagged->metadata !== ['team' => 'brand']) {
        throw new RuntimeException('metadata replacement changed the NUID or did not replace the bag');
    }

    // addLink() creates an alias object: it stores no content (size 0, chunks 0), only the target.
    $link = $store->addLink('latest-report', 'report-2026.txt')->await();
    if (!$link->isLink() || ($link->link['name'] ?? '') !== 'report-2026.txt' || $link->size !== 0) {
        throw new RuntimeException('addLink did not create a link to report-2026.txt');
    }

    // Reading the link follows it to the target (in this or another bucket) and returns the target bytes.
    $throughLink = $store->get('latest-report')->await();
    if ($throughLink === null || $throughLink->data !== $payload) {
        throw new RuntimeException('reading through the link did not return the target object bytes');
    }

    // addBucketLink() points at a whole bucket instead of a single object, so its link carries a bucket
    // and no object name. It is a pointer for tooling; get() on it has no object to read.
    $bucketLink = $store->addBucketLink('archive', $sealedBucket)->await();
    if (!$bucketLink->isLink() || ($bucketLink->link['bucket'] ?? '') !== $sealedBucket || isset($bucketLink->link['name'])) {
        throw new RuntimeException('addBucketLink did not create a bucket link to ' . $sealedBucket);
    }

    // The addLink guard (nats.go parity): a name held by ANY non-link record is refused, because
    // publishing the link meta would replace that record and strand its chunks. Two cases follow.

    // 1. A live stored object holds the name.
    $store->put('notes.txt', "draft notes\n")->await();
    try {
        $store->addLink('notes.txt', 'report-2026.txt')->await();
    } catch (JetStreamException $e) {
        // Only the already-exists guard counts as a demonstration; anything else is a real failure.
        if (!str_contains($e->getMessage(), 'already exists')) {
            throw $e;
        }

        $rejectedLinkNames[] = 'notes.txt';
    }

    // 2. A deleted tombstone holds the name - here the one the rename left behind. The record still
    // exists, so linking over it is refused as well.
    try {
        $store->addLink('report.txt', 'report-2026.txt')->await();
    } catch (JetStreamException $e) {
        if (!str_contains($e->getMessage(), 'already exists')) {
            throw $e;
        }

        $rejectedLinkNames[] = 'report.txt';
    }

    if (count($rejectedLinkNames) !== 2) {
        throw new RuntimeException('addLink accepted a name already held by an object record');
    }

    // seal() makes a bucket permanently read-only. It cannot be undone, so it runs against the
    // throwaway bucket only - the bucket above is still needed for reads.
    $sealedStore->put('archived.txt', "frozen copy\n")->await();
    $sealedStore->seal()->await();

    // A write to a sealed bucket is rejected by the server ("invalid operation on sealed stream").
    $sealRejected = false;
    try {
        $sealedStore->put('another.txt', "too late\n")->await();
    } catch (JetStreamException $e) {
        if (!str_contains($e->getMessage(), 'sealed')) {
            throw $e;
        }

        $sealRejected = true;
    }

    if (!$sealRejected) {
        throw new RuntimeException('the sealed bucket still accepted a write');
    }

    // Reads keep working on a sealed bucket.
    $frozen = $sealedStore->get('archived.txt')->await();
    if ($frozen === null || $frozen->data !== "frozen copy\n") {
        throw new RuntimeException('the sealed bucket did not serve its stored object');
    }

    echo 'OK object-store-links-and-meta: renamed report.txt->report-2026.txt keeping nuid ' . $renamed->nuid
        . ' (' . $renamed->chunks . ' chunks, ' . $renamed->size . ' bytes), metadata now team='
        . ($retagged->metadata['team'] ?? '') . ', link latest-report->' . ($link->link['name'] ?? '')
        . ', bucket link archive->' . ($bucketLink->link['bucket'] ?? '')
        . ', addLink rejected [' . implode(',', $rejectedLinkNames) . '], sealed ' . $sealedBucket . "\n";
} finally {
    foreach ([$store, $sealedStore] as $created) {
        try {
            // A sealed bucket rejects writes but can still be dropped, so both go away here.
            $created->deleteBucket()->await();
        } catch (\Throwable) {
            // best-effort cleanup
        }
    }

    $client->disconnect()->await();
}
