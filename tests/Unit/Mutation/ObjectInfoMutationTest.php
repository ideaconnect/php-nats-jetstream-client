<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\JetStream\ObjectStore\ObjectInfo;
use PHPUnit\Framework\TestCase;

/**
 * Mutation-killing unit tests for {@see ObjectInfo::fromArray()}. ObjectInfo is a pure immutable
 * value object built from a decoded meta record, so each test feeds a hand-crafted array and pins
 * the exact field/boundary a surviving Infection mutant would change. Source must NOT be modified.
 */
final class ObjectInfoMutationTest extends TestCase
{
    /**
     * fromArray() coerces every metadata value through strval(). The mutant drops the array_map and
     * returns the raw values, so a non-string value would survive un-coerced.
     */
    public function testMetadataValuesAreStringCoerced(): void
    {
        // kills UnwrapArrayMap @ line 59
        $info = ObjectInfo::fromArray('B', ['metadata' => ['n' => 7, 'b' => true]]);

        self::assertSame('7', $info->metadata['n']);
        self::assertSame('1', $info->metadata['b']);
    }

    /**
     * Both the array check AND the bucket-key presence must hold for a link to be produced. The
     * mutant uses ||, which would build a (bogus, empty-bucket) link from a link array that has no
     * bucket key. With the real && the result stays a non-link.
     */
    public function testLinkArrayWithoutBucketKeyIsNotALink(): void
    {
        // kills LogicalAnd @ line 64
        $info = ObjectInfo::fromArray('B', ['options' => ['link' => ['name' => 'only-name']]]);

        self::assertNull($info->link);
        self::assertFalse($info->isLink());
    }

    /**
     * A well-formed link array yields a link (proves the && true-branch is genuinely reached and
     * not e.g. always-null), and the bucket value is string-cast.
     */
    public function testLinkBucketIsStringCast(): void
    {
        // kills CastString @ line 65
        $info = ObjectInfo::fromArray('B', ['options' => ['link' => ['bucket' => 12345]]]);

        self::assertNotNull($info->link);
        self::assertTrue($info->isLink());
        self::assertTrue(isset($info->link['bucket']));
        self::assertSame('12345', $info->link['bucket']);
    }

    /**
     * The optional link name is also string-cast when present and non-empty.
     */
    public function testLinkNameIsStringCast(): void
    {
        // kills CastString @ line 67
        $info = ObjectInfo::fromArray('B', ['options' => ['link' => ['bucket' => 'tgt', 'name' => 999]]]);

        self::assertNotNull($info->link);
        self::assertTrue(isset($info->link['bucket']));
        self::assertSame('tgt', $info->link['bucket']);
        self::assertTrue(isset($info->link['name']));
        self::assertSame('999', $info->link['name']);
    }

    /**
     * bucket prefers $data['bucket'] over the $bucket argument. The mutant swaps the coalesce
     * operands so it would always return the $bucket argument and ignore the record value.
     */
    public function testBucketFromDataTakesPrecedenceOverArgument(): void
    {
        // kills Coalesce @ line 72
        $info = ObjectInfo::fromArray('arg-bucket', ['bucket' => 'data-bucket']);

        self::assertSame('data-bucket', $info->bucket);
    }

    /**
     * Missing size defaults to exactly 0. Increment/Decrement mutants shift the default to 1/-1.
     */
    public function testMissingSizeDefaultsToZero(): void
    {
        // kills IncrementInteger + DecrementInteger @ line 74
        $info = ObjectInfo::fromArray('B', []);

        self::assertSame(0, $info->size);
    }

    /**
     * Missing chunks defaults to exactly 0. Increment/Decrement mutants shift the default to 1/-1.
     */
    public function testMissingChunksDefaultsToZero(): void
    {
        // kills IncrementInteger + DecrementInteger @ line 75
        $info = ObjectInfo::fromArray('B', []);

        self::assertSame(0, $info->chunks);
    }

    /**
     * modified comes from $data['mtime']. The mutant swaps the coalesce operands ('' ?? mtime) so it
     * would always be empty regardless of the record.
     */
    public function testModifiedReadsMtimeFromData(): void
    {
        // kills Coalesce @ line 77
        $info = ObjectInfo::fromArray('B', ['mtime' => '2026-06-14T00:00:00Z']);

        self::assertSame('2026-06-14T00:00:00Z', $info->modified);
    }

    /**
     * Missing deleted defaults to false. The mutant flips the default to true.
     */
    public function testMissingDeletedDefaultsToFalse(): void
    {
        // kills FalseValue @ line 78
        $info = ObjectInfo::fromArray('B', []);

        self::assertFalse($info->deleted);
    }
}
