<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\ObjectData;
use IDCT\NATS\JetStream\ObjectInfo;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

final class ObjectStoreBucketTest extends TestCase
{
    /**
     * Verifies Object Store bucket create/delete map to stream lifecycle APIs.
     */
    public function testBucketCreateAndDelete(): void
    {
        $createPayload = '{"config":{"name":"OBJ_assets","subjects":["$O.assets.C.>","$O.assets.M.>"]}}';
        $deletePayload = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($deletePayload), $deletePayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $bucket = $client->jetStream()->objectStore('assets');
        $created = $bucket->create()->await();
        $deleted = $bucket->deleteBucket()->await();

        self::assertSame('OBJ_assets', $created->name);
        self::assertTrue($deleted);
        self::assertStringContainsString('$JS.API.STREAM.CREATE.OBJ_assets', $transport->writes[3]);
        self::assertStringContainsString('$JS.API.STREAM.DELETE.OBJ_assets', $transport->writes[6]);
    }

    /**
     * Verifies put/get/object info flow using metadata and chunk subjects.
     */
    public function testPutGetAndInfo(): void
    {
        $chunkAck = '{"stream":"OBJ_assets","seq":1,"duplicate":false}';
        $metaAck = '{"stream":"OBJ_assets","seq":2,"duplicate":false}';

        $meta = [
            'name' => 'logo.txt',
            'size' => 5,
            'digest' => 'SHA-256=' . base64_encode(hash('sha256', 'hello', true)),
            'mtime' => '2030-01-01T00:00:00Z',
            'deleted' => false,
            'chunk_subject' => '$O.assets.C.abcd',
            'metadata' => ['content-type' => 'text/plain'],
        ];

        $metaGetPayload = json_encode([
            'message' => [
                'subject' => '$O.assets.M.logo.txt',
                'seq' => 2,
                'data' => base64_encode((string) json_encode($meta, JSON_THROW_ON_ERROR)),
            ],
        ], JSON_THROW_ON_ERROR);

        $chunkGetPayload = json_encode([
            'message' => [
                'subject' => '$O.assets.C.abcd',
                'seq' => 1,
                'data' => base64_encode('hello'),
            ],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($chunkAck), $chunkAck),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($metaAck), $metaAck),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($metaGetPayload), $metaGetPayload),
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($chunkGetPayload), $chunkGetPayload),
            sprintf("MSG _INBOX.e 5 %d\r\n%s\r\n", strlen($metaGetPayload), $metaGetPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $bucket = $client->jetStream()->objectStore('assets');
        $stored = $bucket->put('logo.txt', 'hello', ['content-type' => 'text/plain'])->await();
        $fetched = $bucket->get('logo.txt')->await();
        $info = $bucket->info('logo.txt')->await();

        self::assertInstanceOf(ObjectInfo::class, $stored);
        self::assertSame('logo.txt', $stored->name);
        self::assertInstanceOf(ObjectData::class, $fetched);
        self::assertNotNull($fetched);
        self::assertSame('hello', $fetched->data);
        self::assertSame('logo.txt', $fetched->info->name);
        self::assertInstanceOf(ObjectInfo::class, $info);
        self::assertSame('text/plain', $info?->metadata['content-type'] ?? null);

        self::assertStringStartsWith('PUB $O.assets.C.', $transport->writes[3]);
        self::assertStringStartsWith('PUB $O.assets.M.logo.txt', $transport->writes[6]);
    }

    /**
     * Verifies delete writes tombstone metadata and get returns deleted object.
     */
    public function testDeleteTombstoneAndGetDeletedObject(): void
    {
        $deleteAck = '{"stream":"OBJ_assets","seq":5,"duplicate":false}';
        $deletedMeta = [
            'name' => 'logo.txt',
            'size' => 0,
            'digest' => '',
            'mtime' => '2030-01-01T00:00:00Z',
            'deleted' => true,
            'chunk_subject' => '',
            'metadata' => [],
        ];

        $deletedMetaPayload = json_encode([
            'message' => [
                'subject' => '$O.assets.M.logo.txt',
                'seq' => 5,
                'data' => base64_encode((string) json_encode($deletedMeta, JSON_THROW_ON_ERROR)),
            ],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($deleteAck), $deleteAck),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($deletedMetaPayload), $deletedMetaPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $bucket = $client->jetStream()->objectStore('assets');
        $deletedInfo = $bucket->delete('logo.txt')->await();
        $fetched = $bucket->get('logo.txt')->await();

        self::assertTrue($deletedInfo->deleted);
        self::assertInstanceOf(ObjectData::class, $fetched);
        self::assertTrue($fetched->info->deleted);
        self::assertNull($fetched->data);
        self::assertStringStartsWith('PUB $O.assets.M.logo.txt', $transport->writes[3]);
    }

    /**
     * Verifies invalid object names are rejected.
     */
    public function testInvalidObjectNameRejected(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid object name');

        $client->jetStream()->objectStore('assets')->put('bad name', 'x')->await();
    }

    /**
     * Verifies object listing returns latest metadata and filters tombstones by default.
     */
    public function testListAndStatus(): void
    {
        $streamInfo = json_encode([
            'config' => ['name' => 'OBJ_assets'],
            'state' => [
                'messages' => 4,
                'last_seq' => 4,
                'bytes' => 123,
                'subjects' => [
                    '$O.assets.M.logo.txt' => 2,
                    '$O.assets.M.old.txt' => 1,
                    '$O.assets.C.chunk1' => 1,
                ],
            ],
        ], JSON_THROW_ON_ERROR);

        $logoMeta = [
            'name' => 'logo.txt',
            'size' => 5,
            'digest' => 'SHA-256=' . base64_encode(hash('sha256', 'hello', true)),
            'mtime' => '2030-01-01T00:00:00Z',
            'deleted' => false,
            'chunk_subject' => '$O.assets.C.chunk1',
            'metadata' => ['content-type' => 'text/plain'],
        ];

        $oldMeta = [
            'name' => 'old.txt',
            'size' => 0,
            'digest' => '',
            'mtime' => '2030-01-01T00:00:00Z',
            'deleted' => true,
            'chunk_subject' => '',
            'metadata' => [],
        ];

        $seq1ChunkPayload = json_encode([
            'message' => [
                'subject' => '$O.assets.C.chunk1',
                'seq' => 1,
                'data' => base64_encode('hello'),
            ],
        ], JSON_THROW_ON_ERROR);

        $seq2LogoPayload = json_encode([
            'message' => [
                'subject' => '$O.assets.M.logo.txt',
                'seq' => 2,
                'data' => base64_encode((string) json_encode($logoMeta, JSON_THROW_ON_ERROR)),
            ],
        ], JSON_THROW_ON_ERROR);

        $seq3OldPayload = json_encode([
            'message' => [
                'subject' => '$O.assets.M.old.txt',
                'seq' => 3,
                'data' => base64_encode((string) json_encode($oldMeta, JSON_THROW_ON_ERROR)),
            ],
        ], JSON_THROW_ON_ERROR);

        $seq4LogoNewPayload = json_encode([
            'message' => [
                'subject' => '$O.assets.M.logo.txt',
                'seq' => 4,
                'data' => base64_encode((string) json_encode($logoMeta, JSON_THROW_ON_ERROR)),
            ],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen((string) $streamInfo), (string) $streamInfo),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($seq1ChunkPayload), $seq1ChunkPayload),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($seq2LogoPayload), $seq2LogoPayload),
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($seq3OldPayload), $seq3OldPayload),
            sprintf("MSG _INBOX.e 5 %d\r\n%s\r\n", strlen($seq4LogoNewPayload), $seq4LogoNewPayload),
            sprintf("MSG _INBOX.f 6 %d\r\n%s\r\n", strlen((string) $streamInfo), (string) $streamInfo),
            sprintf("MSG _INBOX.g 7 %d\r\n%s\r\n", strlen($seq1ChunkPayload), $seq1ChunkPayload),
            sprintf("MSG _INBOX.h 8 %d\r\n%s\r\n", strlen($seq2LogoPayload), $seq2LogoPayload),
            sprintf("MSG _INBOX.i 9 %d\r\n%s\r\n", strlen($seq3OldPayload), $seq3OldPayload),
            sprintf("MSG _INBOX.j 10 %d\r\n%s\r\n", strlen($seq4LogoNewPayload), $seq4LogoNewPayload),
            sprintf("MSG _INBOX.k 11 %d\r\n%s\r\n", strlen((string) $streamInfo), (string) $streamInfo),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $bucket = $client->jetStream()->objectStore('assets');
        $activeObjects = $bucket->list()->await();
        $allObjects = $bucket->list(includeDeleted: true)->await();
        $status = $bucket->getStatus()->await();

        self::assertCount(1, $activeObjects);
        self::assertSame('logo.txt', $activeObjects[0]->name);
        self::assertFalse($activeObjects[0]->deleted);

        self::assertCount(2, $allObjects);
        self::assertSame('OBJ_assets', $status['stream']);
        self::assertSame(4, $status['last_sequence']);
        self::assertSame(4, $status['messages']);
    }

    // ─── Name Validation ─────────────────────────────────────────────

    public function testPutAcceptsNameWithDotsColonsSlashes(): void
    {
        $chunkAck = '{"stream":"OBJ_assets","seq":1,"duplicate":false}';
        $metaAck = '{"stream":"OBJ_assets","seq":2,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($chunkAck), $chunkAck),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($metaAck), $metaAck),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $info = $client->jetStream()->objectStore('assets')->put('images/logo:v2.png', 'data')->await();
        self::assertSame('images/logo:v2.png', $info->name);
    }

    public function testPutRejectsNameWithWildcard(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid object name');
        $client->jetStream()->objectStore('assets')->put('img*', 'data')->await();
    }

    public function testPutRejectsNameWithTab(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid object name');
        $client->jetStream()->objectStore('assets')->put("img\there", 'data')->await();
    }

    // ─── Chunking ───────────────────────────────────────────────────

    public function testPutChunksLargeObject(): void
    {
        // Create data larger than 10 bytes to trigger chunking with custom chunk size.
        // We can't easily control chunkSize via JetStreamContext, but we can verify
        // the metadata includes chunks count in the published metadata payload.
        $chunkAck1 = '{"stream":"OBJ_assets","seq":1,"duplicate":false}';
        $metaAck = '{"stream":"OBJ_assets","seq":2,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            // Single chunk (data fits in default 128KB)
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($chunkAck1), $chunkAck1),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($metaAck), $metaAck),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $info = $client->jetStream()->objectStore('assets')->put('doc.txt', 'hello world')->await();

        self::assertSame('doc.txt', $info->name);
        self::assertSame(11, $info->size);
        self::assertSame(1, $info->chunks);

        // Verify metadata payload in transport writes contains chunks field
        $allWrites = implode('|', $transport->writes);
        self::assertStringContainsString('"chunks":1', $allWrites);
        self::assertStringContainsString('"size":11', $allWrites);
    }

    public function testObjectInfoIncludesChunksField(): void
    {
        $info = ObjectInfo::fromArray('assets', [
            'name' => 'big.bin',
            'size' => 300000,
            'chunks' => 3,
            'digest' => 'SHA-256=abc',
            'mtime' => '2026-01-01T00:00:00Z',
            'deleted' => false,
            'chunk_subject' => '$O.assets.C.deadbeef',
        ]);

        self::assertSame(3, $info->chunks);
        self::assertSame(300000, $info->size);
    }

    public function testGetThrowsOnDigestMismatch(): void
    {
        $correctData = 'hello world';
        $corruptedData = 'CORRUPTED!!';
        $digest = 'SHA-256=' . base64_encode(hash('sha256', $correctData, true));
        $chunkSubject = '$O.assets.C.deadbeef';

        $metadata = json_encode([
            'name' => 'doc.txt',
            'size' => strlen($correctData),
            'chunks' => 1,
            'digest' => $digest,
            'mtime' => '2026-01-01T00:00:00Z',
            'deleted' => false,
            'chunk_subject' => $chunkSubject,
            'metadata' => [],
        ], JSON_THROW_ON_ERROR);

        $metaResponse = json_encode([
            'message' => ['data' => base64_encode($metadata), 'subject' => '$O.assets.M.doc.txt'],
        ], JSON_THROW_ON_ERROR);

        // Return corrupted chunk data instead of correct data.
        $chunkResponse = json_encode([
            'message' => ['data' => base64_encode($corruptedData), 'subject' => $chunkSubject],
        ], JSON_THROW_ON_ERROR);

        // info() request subscribes SID 1, get chunk request subscribes SID 2
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.any 1 %d\r\n%s\r\n", strlen($metaResponse), $metaResponse),
            sprintf("MSG _INBOX.any 2 %d\r\n%s\r\n", strlen($chunkResponse), $chunkResponse),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Object digest mismatch');
        $client->jetStream()->objectStore('assets')->get('doc.txt')->await();
    }
}
