<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\ObjectStore\ObjectData;
use IDCT\NATS\JetStream\ObjectStore\ObjectInfo;
use IDCT\NATS\JetStream\ObjectStore\ObjectStoreBucket;
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

        $ephemeralConsumerPayload = '{"stream_name":"OBJ_assets","name":"EPH1","config":{"ack_policy":"explicit","filter_subject":"$O.assets.C.abcd"}}';
        $deleteConsumerPayload = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($chunkAck), $chunkAck),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($metaAck), $metaAck),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($metaGetPayload), $metaGetPayload),
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($ephemeralConsumerPayload), $ephemeralConsumerPayload),
            "MSG _INBOX.JS.FETCH.a 5 5\r\nhello\r\n",
            sprintf("MSG _INBOX.e 6 %d\r\n%s\r\n", strlen($deleteConsumerPayload), $deleteConsumerPayload),
            sprintf("MSG _INBOX.f 7 %d\r\n%s\r\n", strlen($metaGetPayload), $metaGetPayload),
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
        self::assertSame('hello', $fetched->data);
        self::assertSame('logo.txt', $fetched->info->name);
        self::assertInstanceOf(ObjectInfo::class, $info);
        self::assertSame('text/plain', $info->metadata['content-type'] ?? null);

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
        $ephemeralConsumerPayload = '{"stream_name":"OBJ_assets","name":"EPH2","config":{"ack_policy":"explicit","filter_subject":"$O.assets.C.deadbeef"}}';
        $deleteConsumerPayload = '{"success":true}';

        // info() request subscribes SID 1, then create ephemeral consumer + pull fetch + delete.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.any 1 %d\r\n%s\r\n", strlen($metaResponse), $metaResponse),
            sprintf("MSG _INBOX.any 2 %d\r\n%s\r\n", strlen($ephemeralConsumerPayload), $ephemeralConsumerPayload),
            sprintf("MSG _INBOX.JS.FETCH.a 3 %d\r\n%s\r\n", strlen($corruptedData), $corruptedData),
            sprintf("MSG _INBOX.any 4 %d\r\n%s\r\n", strlen($deleteConsumerPayload), $deleteConsumerPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Object digest mismatch');
        $client->jetStream()->objectStore('assets')->get('doc.txt')->await();
    }

    /**
     * Verifies getToCallback forwards downloaded payload chunks to the callback.
     */
    public function testGetToCallbackPassesPayloadToHandler(): void
    {
        $meta = [
            'name' => 'report.txt',
            'size' => 5,
            'chunks' => 1,
            'digest' => 'SHA-256=' . base64_encode(hash('sha256', 'hello', true)),
            'mtime' => '2030-01-01T00:00:00Z',
            'deleted' => false,
            'chunk_subject' => '$O.assets.C.cb1',
            'metadata' => [],
        ];

        $metaPayload = json_encode([
            'message' => [
                'subject' => '$O.assets.M.report.txt',
                'seq' => 1,
                'data' => base64_encode((string) json_encode($meta, JSON_THROW_ON_ERROR)),
            ],
        ], JSON_THROW_ON_ERROR);

        $ephemeralConsumerPayload = '{"stream_name":"OBJ_assets","name":"EPH3","config":{"ack_policy":"explicit","filter_subject":"$O.assets.C.cb1"}}';
        $deleteConsumerPayload = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($metaPayload), $metaPayload),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($ephemeralConsumerPayload), $ephemeralConsumerPayload),
            "MSG _INBOX.JS.FETCH.a 3 5\r\nhello\r\n",
            sprintf("MSG _INBOX.c 4 %d\r\n%s\r\n", strlen($deleteConsumerPayload), $deleteConsumerPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $captured = '';
        $info = $client->jetStream()->objectStore('assets')->getToCallback(
            'report.txt',
            static function (string $chunk) use (&$captured): void {
                $captured .= $chunk;
            },
        )->await();

        self::assertSame('hello', $captured);
        self::assertNotNull($info);
        self::assertSame('report.txt', $info->name);
    }

    /**
     * Verifies info/get return null when JetStream reports object metadata not found.
     */
    public function testInfoAndGetReturnNullWhenObjectNotFound(): void
    {
        $missing = '{"error":{"code":404,"description":"message not found"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($missing), $missing),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($missing), $missing),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $bucket = $client->jetStream()->objectStore('assets');
        self::assertNull($bucket->info('missing.txt')->await());
        self::assertNull($bucket->get('missing.txt')->await());
    }

    /**
     * Verifies watch parses metadata payloads and dispatches ObjectInfo updates.
     */
    public function testWatchDispatchesObjectInfo(): void
    {
        $metadata = json_encode([
            'name' => 'logo.txt',
            'size' => 5,
            'chunks' => 1,
            'digest' => '',
            'mtime' => '2030-01-01T00:00:00Z',
            'deleted' => false,
            'chunk_subject' => '$O.assets.C.any',
            'metadata' => [],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG \$O.assets.M.logo.txt 1 %d\r\n%s\r\n", strlen((string) $metadata), (string) $metadata),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $seen = null;
        $sid = $client->jetStream()->objectStore('assets')->watch(static function (ObjectInfo $info) use (&$seen): void {
            $seen = $info;
        })->await();

        self::assertSame(1, $sid);
        self::assertSame(1, $client->processIncoming()->await());
        self::assertInstanceOf(ObjectInfo::class, $seen);
        /** @var ObjectInfo $seenInfo */
        $seenInfo = $seen;
        self::assertSame('logo.txt', $seenInfo->name);
    }

    public function testBucketSubjectHelpers(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $bucket = $client->jetStream()->objectStore('assets');
        self::assertSame('OBJ_assets', $bucket->streamName());
        self::assertSame('$O.assets.C.', $bucket->chunkPrefix());
        self::assertSame('$O.assets.M.', $bucket->metaPrefix());
    }

    public function testListSkipsInvalidMetadataPayloads(): void
    {
        $streamInfo = json_encode([
            'config' => ['name' => 'OBJ_assets'],
            'state' => ['messages' => 3, 'last_seq' => 3, 'bytes' => 1],
        ], JSON_THROW_ON_ERROR);

        $badBase64 = json_encode([
            'message' => ['subject' => '$O.assets.M.bad1', 'seq' => 1, 'data' => '###not-base64###'],
        ], JSON_THROW_ON_ERROR);

        $badJson = json_encode([
            'message' => ['subject' => '$O.assets.M.bad2', 'seq' => 2, 'data' => base64_encode('{bad')],
        ], JSON_THROW_ON_ERROR);

        $emptyData = json_encode([
            'message' => ['subject' => '$O.assets.M.bad3', 'seq' => 3, 'data' => ''],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen((string) $streamInfo), (string) $streamInfo),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($badBase64), $badBase64),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($badJson), $badJson),
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($emptyData), $emptyData),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $listed = $client->jetStream()->objectStore('assets')->list(includeDeleted: true)->await();

        self::assertSame([], $listed);
    }

    public function testListPropagatesNon404SequenceErrors(): void
    {
        $streamInfo = json_encode([
            'config' => ['name' => 'OBJ_assets'],
            'state' => ['messages' => 1, 'last_seq' => 1, 'bytes' => 1],
        ], JSON_THROW_ON_ERROR);
        $errorPayload = '{"error":{"code":500,"description":"sequence read failed"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen((string) $streamInfo), (string) $streamInfo),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($errorPayload), $errorPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('sequence read failed');
        $client->jetStream()->objectStore('assets')->list()->await();
    }

    public function testPutSplitsIntoMultipleChunksWithSmallChunkSize(): void
    {
        $ack1 = '{"stream":"OBJ_assets","seq":1,"duplicate":false}';
        $ack2 = '{"stream":"OBJ_assets","seq":2,"duplicate":false}';
        $ack3 = '{"stream":"OBJ_assets","seq":3,"duplicate":false}';
        $ack4 = '{"stream":"OBJ_assets","seq":4,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ack1), $ack1),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($ack2), $ack2),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($ack3), $ack3),
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($ack4), $ack4),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $bucket = new ObjectStoreBucket($client, $client->jetStream(), 'assets', 4);
        $stored = $bucket->put('multi.bin', 'abcdefghij')->await();

        self::assertSame(3, $stored->chunks);

        $writes = implode('', $transport->writes);
        self::assertSame(3, substr_count($writes, 'PUB $O.assets.C.'));
        self::assertStringContainsString('"chunks":3', $writes);
    }

    public function testGetToCallbackSkipsCallbackForDeletedObjects(): void
    {
        $deletedMeta = [
            'name' => 'gone.txt',
            'size' => 0,
            'digest' => '',
            'mtime' => '2030-01-01T00:00:00Z',
            'deleted' => true,
            'chunk_subject' => '',
            'metadata' => [],
        ];

        $deletedPayload = json_encode([
            'message' => [
                'subject' => '$O.assets.M.gone.txt',
                'seq' => 9,
                'data' => base64_encode((string) json_encode($deletedMeta, JSON_THROW_ON_ERROR)),
            ],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($deletedPayload), $deletedPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $called = false;
        $info = $client->jetStream()->objectStore('assets')->getToCallback(
            'gone.txt',
            static function (string $chunk) use (&$called): void {
                $called = true;
            },
        )->await();

        self::assertFalse($called);
        self::assertNotNull($info);
        self::assertTrue($info->deleted);
    }
}
