<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\JetStream\KeyValue\KeyWatchOptions;

/**
 * Kills the surviving Infection mutants in src/JetStream/KeyValue/KeyWatchOptions.php.
 *
 * Each mutant is pinned by asserting the exact constructor default or the exact
 * toConsumerConfig() output the mutation would change.
 */
final class KeyWatchOptionsMutationTest extends \PHPUnit\Framework\TestCase
{
    /**
     * The `ignoreDeletes` constructor default must be false: by default a watcher
     * delivers delete/purge tombstones. The FalseValue mutant flips the default to
     * true, which would silently suppress deletes for every default-constructed
     * instance.
     */
    public function testIgnoreDeletesDefaultsToFalse(): void
    {
        // kills FalseValue @ line 41
        self::assertFalse(
            (new KeyWatchOptions())->ignoreDeletes,
            'ignoreDeletes must default to false (deletes are delivered unless opted out)',
        );
    }

    /**
     * The `metaOnly` constructor default must be false. metaOnly drives the
     * `headers_only` consumer-config flag, so the FalseValue mutant (default true)
     * is observable both on the property and on the resolved config: a default
     * instance must NOT request headers-only delivery.
     */
    public function testMetaOnlyDefaultsToFalseAndOmitsHeadersOnly(): void
    {
        $options = new KeyWatchOptions();

        // kills FalseValue @ line 42 (property)
        self::assertFalse(
            $options->metaOnly,
            'metaOnly must default to false (value bytes are delivered by default)',
        );

        // kills FalseValue @ line 42 (resolved config side effect)
        $config = $options->toConsumerConfig();
        self::assertArrayNotHasKey(
            'headers_only',
            $config,
            'a default watch must not set headers_only when metaOnly is false',
        );
    }

    /**
     * When metaOnly IS set, headers_only=true is emitted — this anchors the
     * positive side of the metaOnly default so the two states are distinguishable.
     */
    public function testMetaOnlyTrueEmitsHeadersOnly(): void
    {
        $config = (new KeyWatchOptions(metaOnly: true))->toConsumerConfig();

        self::assertArrayHasKey('headers_only', $config);
        self::assertTrue($config['headers_only']);
    }

    /**
     * toConsumerConfig() must always seed the config with ack_policy=none (the KV
     * watch consumer is push/no-ack). The ArrayItemRemoval mutant starts from an
     * empty array, dropping ack_policy entirely.
     */
    public function testConfigAlwaysSetsAckPolicyNone(): void
    {
        // kills ArrayItemRemoval @ line 54 (default instance)
        $defaultConfig = (new KeyWatchOptions())->toConsumerConfig();
        self::assertArrayHasKey('ack_policy', $defaultConfig);
        self::assertSame('none', $defaultConfig['ack_policy']);

        // kills ArrayItemRemoval @ line 54 (also present on the resumeFromRevision branch,
        // which never touches ack_policy itself — proving the seed, not a branch, supplies it)
        $resumeConfig = (new KeyWatchOptions(resumeFromRevision: 7))->toConsumerConfig();
        self::assertArrayHasKey('ack_policy', $resumeConfig);
        self::assertSame('none', $resumeConfig['ack_policy']);
    }
}
