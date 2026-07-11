<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\JetStream\ObjectStore\ObjectStoreConfig;
use PHPUnit\Framework\TestCase;

final class ObjectStoreConfigTest extends TestCase
{
    /**
     * Verifies toStreamConfig() includes 'description' when it is set.
     */
    public function testToStreamConfigIncludesDescription(): void
    {
        $config = new ObjectStoreConfig(description: 'My object store');

        $result = $config->toStreamConfig();

        self::assertSame('My object store', $result['description']);
    }

    /**
     * Verifies toStreamConfig() includes 'placement' when it is set.
     */
    public function testToStreamConfigIncludesPlacement(): void
    {
        $placement = ['cluster' => 'us-east', 'tags' => ['ssd']];
        $config = new ObjectStoreConfig(placement: $placement);

        $result = $config->toStreamConfig();

        self::assertSame($placement, $result['placement']);
    }

    /**
     * Verifies toStreamConfig() maps all non-null fields to their stream-config keys.
     */
    public function testToStreamConfigMapsAllFields(): void
    {
        $placement = ['cluster' => 'eu-west'];
        $config = new ObjectStoreConfig(
            ttlSeconds: 3600,
            maxBytes: 1048576,
            storage: 'memory',
            replicas: 3,
            compression: 's2',
            description: 'Full config',
            placement: $placement,
        );

        $result = $config->toStreamConfig();

        self::assertSame(3600 * 1_000_000_000, $result['max_age']);
        self::assertSame(1048576, $result['max_bytes']);
        self::assertSame('memory', $result['storage']);
        self::assertSame(3, $result['num_replicas']);
        self::assertSame('s2', $result['compression']);
        self::assertSame('Full config', $result['description']);
        self::assertSame($placement, $result['placement']);
    }

    /**
     * Verifies toStreamConfig() returns an empty array when no fields are set.
     */
    public function testToStreamConfigReturnsEmptyArrayForDefaultInstance(): void
    {
        $config = new ObjectStoreConfig();

        self::assertSame([], $config->toStreamConfig());
    }
}
