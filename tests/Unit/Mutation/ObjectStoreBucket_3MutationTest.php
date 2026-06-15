<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\ObjectStore\ObjectInfo;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

/**
 * Targeted unit tests whose sole purpose is to kill surviving Infection mutants in
 * src/JetStream/ObjectStore/ObjectStoreBucket.php (chunk 3). Each assertion group pins the exact
 * observable behaviour a specific mutation would break.
 */
final class ObjectStoreBucket_3MutationTest extends TestCase
{
    /** URL-safe base64 (with padding), matching the official Object Store meta-subject encoding. */
    private function encodeName(string $name): string
    {
        return strtr(base64_encode($name), '+/', '-_');
    }

    private function digestOf(string $data): string
    {
        return 'SHA-256=' . strtr(base64_encode(hash('sha256', $data, true)), '+/', '-_');
    }

    /**
     * STREAM.MSG.GET envelope (base64 data body) used by the fetchInfo() leader fallback path.
     *
     * @param array<string,mixed> $extra
     */
    private function metaGetResponse(string $name, array $extra, int $seq = 2): string
    {
        $meta = array_merge([
            'name' => $name,
            'bucket' => 'assets',
            'mtime' => '2030-01-01T00:00:00Z',
            'deleted' => false,
        ], $extra);

        return (string) json_encode([
            'message' => [
                'subject' => '$O.assets.M.' . $this->encodeName($name),
                'seq' => $seq,
                'data' => base64_encode((string) json_encode($meta, JSON_THROW_ON_ERROR)),
            ],
        ], JSON_THROW_ON_ERROR);
    }

    private function pubAck(int $seq): string
    {
        return sprintf('{"stream":"OBJ_assets","seq":%d,"duplicate":false}', $seq);
    }

    private function notFound(): string
    {
        return '{"error":{"code":404,"description":"message not found"}}';
    }

    /** Direct Get reply (HMSG) whose body is the raw meta JSON, as info()/get() read it. */
    private function directMetaReply(string $envelope, int $sid, int $seq = 2): string
    {
        /** @var array{message: array{data?: string}} $decoded */
        $decoded = json_decode($envelope, true, 512, JSON_THROW_ON_ERROR);
        $metaJson = (string) base64_decode((string) ($decoded['message']['data'] ?? ''), true);
        $hdrs = "NATS/1.0\r\nNats-Stream: OBJ_assets\r\nNats-Sequence: {$seq}\r\n\r\n";
        $h = strlen($hdrs);

        return sprintf("HMSG _INBOX.x %d %d %d\r\n%s%s\r\n", $sid, $h, $h + strlen($metaJson), $hdrs, $metaJson);
    }

    /** Direct Get reply (HMSG) for a single object chunk (raw bytes body on the NUID subject). */
    private function directChunkReply(string $nuid, string $payload, int $sid): string
    {
        $hdrs = "NATS/1.0\r\nNats-Stream: OBJ_assets\r\nNats-Subject: \$O.assets.C.{$nuid}\r\nNats-Sequence: 3\r\n\r\n";
        $h = strlen($hdrs);

        return sprintf("HMSG _INBOX.x %d %d %d\r\n%s%s\r\n", $sid, $h, $h + strlen($payload), $hdrs, $payload);
    }

    /** Direct Get status-only reply (HMSG): e.g. a 404 miss or a 503 no-responders. */
    private function directStatusReply(int $sid, int $code, string $description): string
    {
        $hdrs = "NATS/1.0 {$code} {$description}\r\nStatus: {$code}\r\nDescription: {$description}\r\n\r\n";
        $h = strlen($hdrs);

        return sprintf("HMSG _INBOX.x %d %d %d\r\n%s\r\n", $sid, $h, $h, $hdrs);
    }

    private ?FakeTransport $transport = null;

    /** @param list<string> $reads */
    private function connectedClient(array $reads): NatsClient
    {
        $this->transport = new FakeTransport(array_merge([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ], $reads));

        $client = new NatsClient(new NatsOptions(), $this->transport);
        $client->connect()->await();

        return $client;
    }

    /**
     * decodeDigest(): a digest WITHOUT the "SHA-256=" prefix must be rejected (return null), which
     * makes verifyDigest() throw a mismatch. The body after the wrong 8-char prefix decodes to the
     * CORRECT 32 content bytes, so dropping `return null` (ReturnRemoval @ 638) would accept it and
     * let get() succeed - this test fails on that mutant by requiring the mismatch throw.
     */
    public function testGetRejectsDigestWithoutSha256Prefix(): void
    {
        $nuid = 'nuidnopfx01';
        $body = strtr(base64_encode(hash('sha256', 'hello', true)), '+/', '-_'); // valid 32-byte body
        $badDigest = 'BADPFX9=' . $body;                                         // wrong 8-char prefix
        $meta = $this->metaGetResponse('doc.txt', ['nuid' => $nuid, 'size' => 5, 'chunks' => 1, 'digest' => $badDigest]);

        $client = $this->connectedClient([
            $this->directMetaReply($meta, 1),
            $this->directChunkReply($nuid, 'hello', 2),
        ]);

        // kills ReturnRemoval @ 638
        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Object digest mismatch');
        $client->jetStream()->objectStore('assets')->get('doc.txt')->await();
    }

    /**
     * info('') must throw "Invalid object name" (assertValidName @ 660). Removing that call would let
     * the empty name flow into the Direct Get request instead.
     */
    public function testInfoRejectsEmptyName(): void
    {
        $client = $this->connectedClient([]);

        // kills MethodCallRemoval @ 660
        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid object name');
        $client->jetStream()->objectStore('assets')->info('')->await();
    }

    /**
     * info() falls back to the leader STREAM.MSG.GET path on a Direct Get 503; a 404 there means the
     * object is absent and info() returns null. The 404 literal (@ 717) must match exactly: changing
     * it to 403/405 would make fetchInfo() rethrow instead of returning null.
     */
    public function testInfoFallbackTreats404AsAbsent(): void
    {
        $client = $this->connectedClient([
            $this->directStatusReply(1, 503, 'No Responders'),  // Direct Get unavailable -> fallback
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()), // STREAM.MSG.GET -> 404
        ]);

        // kills DecrementInteger/IncrementInteger @ 717 (404 -> 403/405)
        $info = $client->jetStream()->objectStore('assets')->info('missing.txt')->await();
        self::assertNull($info);
    }

    /**
     * info() fallback surfaces the record's stream sequence as ObjectInfo::revision from the
     * STREAM.MSG.GET "seq" field. The Ternary swap (@ 735) would put null in the populated branch.
     */
    public function testInfoFallbackSurfacesRevisionFromSeq(): void
    {
        $meta = $this->metaGetResponse('doc.txt', ['nuid' => 'n1', 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')], 42);

        $client = $this->connectedClient([
            $this->directStatusReply(1, 503, 'No Responders'),                  // Direct Get -> fallback
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($meta), $meta),       // STREAM.MSG.GET success, seq=42
        ]);

        $info = $client->jetStream()->objectStore('assets')->info('doc.txt')->await();

        self::assertNotNull($info);
        // kills Ternary @ 735 (would yield null when seq is set)
        self::assertSame(42, $info->revision);
    }

    /**
     * delete('') must throw "Invalid object name" (assertValidName @ 748).
     */
    public function testDeleteRejectsEmptyName(): void
    {
        $client = $this->connectedClient([]);

        // kills MethodCallRemoval @ 748
        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid object name');
        $client->jetStream()->objectStore('assets')->delete('')->await();
    }

    /**
     * delete() writes a tombstone meta record. Pin the exact fields of the published JSON so the
     * array-construction mutants on lines 753-762 are caught: name, bucket, size:0, chunks:0, and the
     * options.max_chunk_size entry.
     */
    public function testDeleteTombstoneFieldsArePinned(): void
    {
        $client = $this->connectedClient([
            // No chunk to await first, so the tombstone publish is issued before the concurrent lookup.
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->pubAck(7)), $this->pubAck(7)),       // tombstone ack (sid 1)
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),     // lookup -> 404 (sid 2)
        ]);

        $store = new \IDCT\NATS\JetStream\ObjectStore\ObjectStoreBucket($client, $client->jetStream(), 'assets', 4096);
        $deleted = $store->delete('logo.txt')->await();

        self::assertTrue($deleted->deleted);

        $tombstone = $this->lastMetaPublish($this->encodeName('logo.txt'));

        // kills ArrayItemRemoval @ 753 (name dropped)
        self::assertStringContainsString('"name":"logo.txt"', $tombstone);
        // kills ArrayItem @ 755 ('bucket' => -> 'bucket' >)
        self::assertStringContainsString('"bucket":"assets"', $tombstone);
        // kills DecrementInteger/IncrementInteger @ 757 (size 0 -> -1/1)
        self::assertStringContainsString('"size":0', $tombstone);
        // kills DecrementInteger/IncrementInteger @ 758 (chunks 0 -> -1/1)
        self::assertStringContainsString('"chunks":0', $tombstone);
        // kills ArrayItem/ArrayItemRemoval @ 762 (options.max_chunk_size dropped/broken)
        self::assertStringContainsString('"max_chunk_size":4096', $tombstone);
        self::assertStringContainsString('"options":{"max_chunk_size":4096}', $tombstone);
    }

    /**
     * updateMeta('') must throw "Invalid object name" (assertValidName @ 793).
     */
    public function testUpdateMetaRejectsEmptyName(): void
    {
        $client = $this->connectedClient([]);

        // kills MethodCallRemoval @ 793
        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid object name');
        $client->jetStream()->objectStore('assets')->updateMeta('')->await();
    }

    /**
     * updateMeta() of a missing object throws JetStreamException with code 404 (@ 797). The literal
     * must stay 404; 403/405 would change the surfaced code.
     */
    public function testUpdateMetaMissingObjectThrowsWith404Code(): void
    {
        $client = $this->connectedClient([
            $this->directStatusReply(1, 404, 'Message Not Found'), // info() -> not found
        ]);

        try {
            $client->jetStream()->objectStore('assets')->updateMeta('ghost.txt', 'other.txt')->await();
            self::fail('Expected JetStreamException for a missing object');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('Object not found', $e->getMessage());
            // kills DecrementInteger/IncrementInteger @ 797 (404 -> 403/405)
            self::assertSame(404, $e->getCode());
        }
    }

    /**
     * updateMeta() rename to an EMPTY new name must be rejected by assertValidName($newName) (@ 802).
     * The existing object is found first, so isRename is true and the new-name validation is reached.
     */
    public function testUpdateMetaRenameToEmptyNameIsRejected(): void
    {
        $meta = $this->metaGetResponse('logo.txt', ['nuid' => 'n1', 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('old')]);

        $client = $this->connectedClient([
            $this->directMetaReply($meta, 1), // info('logo.txt') -> exists
        ]);

        // kills MethodCallRemoval @ 802
        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid object name');
        $client->jetStream()->objectStore('assets')->updateMeta('logo.txt', '')->await();
    }

    /**
     * updateMeta() in-place metadata replace writes the new meta carrying the EXISTING bucket, chunks,
     * digest and the options.max_chunk_size entry. Pins the array-construction mutants on lines
     * 812-819.
     */
    public function testUpdateMetaInPlaceMetaFieldsArePinned(): void
    {
        $meta = $this->metaGetResponse('logo.txt', [
            'nuid' => 'n1',
            'size' => 3,
            'chunks' => 1,
            'digest' => $this->digestOf('old'),
            'metadata' => ['team' => 'design'],
        ]);

        $client = $this->connectedClient([
            $this->directMetaReply($meta, 1),                                                  // info('logo.txt')
            sprintf("MSG _INBOX.c 2 %d\r\n%s\r\n", strlen($this->pubAck(8)), $this->pubAck(8)), // publishMeta ack
        ]);

        $store = new \IDCT\NATS\JetStream\ObjectStore\ObjectStoreBucket($client, $client->jetStream(), 'assets', 4096);
        $info = $store->updateMeta('logo.txt', null, ['team' => 'brand'])->await();
        self::assertSame('logo.txt', $info->name);

        $published = $this->lastMetaPublish($this->encodeName('logo.txt'));

        // kills ArrayItem @ 812 ('bucket' => -> 'bucket' >)
        self::assertStringContainsString('"bucket":"assets"', $published);
        // kills ArrayItem @ 815 ('chunks' => $existing->chunks -> 'chunks' > ...)
        self::assertStringContainsString('"chunks":1', $published);
        // kills ArrayItem @ 816 ('digest' => $existing->digest -> 'digest' > ...)
        self::assertStringContainsString('"digest":"' . $this->digestOf('old') . '"', $published);
        // kills ArrayItem/ArrayItemRemoval @ 819 (options.max_chunk_size dropped/broken)
        self::assertStringContainsString('"options":{"max_chunk_size":4096}', $published);
    }

    /**
     * updateMeta() rename tombstones the OLD name. Pin the old-name tombstone fields so the
     * array-construction mutants on lines 828-837 are caught: name, bucket, size:0, chunks:0, options.
     */
    public function testUpdateMetaRenameTombstoneFieldsArePinned(): void
    {
        $meta = $this->metaGetResponse('logo.txt', ['nuid' => 'n1', 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('old')]);

        $client = $this->connectedClient([
            $this->directMetaReply($meta, 1),                                                  // info('logo.txt') -> exists
            $this->directStatusReply(2, 404, 'Message Not Found'),                             // info('brand.txt') clash -> free
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(8)), $this->pubAck(8)), // publishMeta('brand.txt') ack
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($this->pubAck(9)), $this->pubAck(9)), // publishMeta(old tombstone) ack
        ]);

        $store = new \IDCT\NATS\JetStream\ObjectStore\ObjectStoreBucket($client, $client->jetStream(), 'assets', 4096);
        $info = $store->updateMeta('logo.txt', 'brand.txt')->await();
        self::assertSame('brand.txt', $info->name);

        $tombstone = $this->lastMetaPublish($this->encodeName('logo.txt'));

        // kills ArrayItemRemoval @ 828 (name dropped from tombstone)
        self::assertStringContainsString('"name":"logo.txt"', $tombstone);
        // kills ArrayItem @ 830 ('bucket' => -> 'bucket' >)
        self::assertStringContainsString('"bucket":"assets"', $tombstone);
        // kills DecrementInteger/IncrementInteger @ 832 (size 0 -> -1/1)
        self::assertStringContainsString('"size":0', $tombstone);
        // kills DecrementInteger/IncrementInteger @ 833 (chunks 0 -> -1/1)
        self::assertStringContainsString('"chunks":0', $tombstone);
        // kills ArrayItem/ArrayItemRemoval @ 837 (options.max_chunk_size dropped/broken)
        self::assertStringContainsString('"options":{"max_chunk_size":4096}', $tombstone);
        // The tombstone marks the old name deleted.
        self::assertStringContainsString('"deleted":true', $tombstone);
    }

    /**
     * watch() builds its consumer filter as metaPrefix() . pattern. With the default pattern '>', the
     * filter must be exactly "$O.assets.M.>". The Concat swap and either ConcatOperandRemoval would
     * produce ">$O.assets.M.", just ">", or "$O.assets.M." (no '>') respectively.
     */
    public function testWatchFilterIsMetaPrefixThenPattern(): void
    {
        $createReply = '{"stream_name":"OBJ_assets","name":"OBJWATCH","config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none"}}';

        $client = $this->connectedClient([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $client->jetStream()->objectStore('assets')->watch(static function (ObjectInfo $info): void {})->await();

        $writes = implode('', $this->writesOf());

        // kills Concat @ 861 (swap), ConcatOperandRemoval @ 861 (pattern only / prefix only)
        self::assertStringContainsString('"filter_subject":"$O.assets.M.>"', $writes);
        self::assertStringNotContainsString('"filter_subject":">$O.assets.M."', $writes);
        self::assertStringNotContainsString('"filter_subject":">"', $writes);
        self::assertStringNotContainsString('"filter_subject":"$O.assets.M."', $writes);
    }

    /**
     * Reads the FakeTransport writes recorded for the active client.
     *
     * @return list<string>
     */
    private function writesOf(): array
    {
        self::assertNotNull($this->transport, 'No transport was created');

        return $this->transport->writes;
    }

    /**
     * Returns the most recent meta-publish frame (HPUB) targeting the given encoded subject token.
     */
    private function lastMetaPublish(string $encodedName): string
    {
        $needle = 'HPUB $O.assets.M.' . $encodedName . ' ';
        $match = '';
        foreach ($this->writesOf() as $write) {
            if (str_contains($write, $needle)) {
                $match = $write;
            }
        }

        self::assertNotSame('', $match, 'No meta publish for ' . $encodedName);

        return $match;
    }
}
