<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Auth\NkeySeedSigner;
use IDCT\NATS\Exception\NatsException;
use PHPUnit\Framework\TestCase;

/**
 * Mutation-killing tests for NkeySeedSigner.
 *
 * Each test pins the exact observable behaviour an Infection mutant would change.
 * Seeds are synthesised with correct CRC16 checksums so the real code accepts them;
 * mutants flip a boundary/bit/match-arm so that they either reject the seed or
 * derive a different public key, which the assertions detect.
 */
final class NkeySeedSignerMutationTest extends TestCase
{
    /** Canonical valid user seed and its expected public key (from the base test). */
    private const SAMPLE_SEED = 'SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY';
    private const SAMPLE_PUBLIC = 'UDXU4RCSJNZOIQHZNWXHXORDPRTGNJAHAHFRGZNEEJCPQTT2M7NLCNF4';

    protected function setUp(): void
    {
        if (!function_exists('sodium_crypto_sign_seed_keypair')) {
            self::markTestSkipped('sodium extension is required for NkeySeedSigner tests.');
        }
    }

    /**
     * trim(strtoupper($seed)) -> trim($seed): without strtoupper the lowercased
     * sample seed contains characters outside the base32 alphabet (a-z), so the
     * mutant throws "Invalid base32 character" while the real code uppercases and
     * accepts it, deriving the canonical public key.
     */
    public function testLowercaseSeedIsUppercasedBeforeDecoding(): void
    {
        // kills UnwrapStrToUpper @ line 77
        $signer = new NkeySeedSigner(strtolower(self::SAMPLE_SEED));

        self::assertSame(self::SAMPLE_PUBLIC, $signer->publicKey());
    }

    /**
     * trim(strtoupper($seed)) -> strtoupper($seed): without trim the leading and
     * trailing whitespace survives into decode() where space is not a base32 char,
     * so the mutant throws. The real code trims and accepts the seed unchanged.
     */
    public function testSurroundingWhitespaceIsTrimmedBeforeDecoding(): void
    {
        // kills UnwrapTrim @ line 77
        $signer = new NkeySeedSigner("  \t" . self::SAMPLE_SEED . " \n");

        self::assertSame(self::SAMPLE_PUBLIC, $signer->publicKey());
    }

    /**
     * b2 = ... | ((ord($raw[1]) & 248) >> 3). The CLUSTER seed below has raw[1] = 128,
     * so the real shift produces b2 = 16 (CLUSTER, valid) and the constructor succeeds.
     *  - >> 3 -> >> 2 gives b2 = 32 (invalid)
     *  - >> 3 -> >> 4 gives b2 = 8  (invalid)
     *  - (248 >> 3) -> (248 << 3) gives b2 = 128 (invalid)
     * All three mutants therefore throw "Invalid NKey seed prefix" while the real
     * code accepts the seed and yields a cluster public key (starts with 'C').
     */
    public function testSeedSecondPrefixByteIsShiftedRightByThree(): void
    {
        // kills DecrementInteger(>>2), IncrementInteger(>>4), ShiftRight(248<<3) @ line 83
        $clusterSeed = 'SCAACAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAKOFQ';

        $signer = new NkeySeedSigner($clusterSeed);

        self::assertStringStartsWith('C', $signer->publicKey());
        self::assertMatchesRegularExpression('/^[A-Z2-7]+$/', $signer->publicKey());
    }

    /**
     * decode(): if (strlen($raw) < 4) throw. The seed below base32-decodes to exactly
     * 4 bytes with a valid CRC. The real check (< 4) passes 4 bytes through, so the
     * later 34-byte guard in decodeSeed() throws "Invalid NKey seed encoding".
     * The mutant (<= 4) rejects the 4-byte buffer earlier with "Invalid NKey encoding".
     * Asserting the *seed* message pins the boundary at exactly 4.
     */
    public function testDecodeAcceptsExactlyFourBytesBeforeChecksumStrip(): void
    {
        // kills LessThan(< 4 -> <= 4) @ line 119
        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('Invalid NKey seed encoding');

        // SAAOWGA decodes to a 4-byte buffer (2-byte payload + 2-byte CRC).
        new NkeySeedSigner('SAAOWGA');
    }

    /**
     * isValidPublicPrefix() match arm for PREFIX_OPERATOR. Removing the OPERATOR arm
     * makes a valid operator seed fall through to default => false, so the constructor
     * throws "Invalid NKey seed prefix". The real code accepts it and the public key
     * starts with 'O'.
     */
    public function testOperatorPrefixSeedIsAccepted(): void
    {
        // kills MatchArmRemoval(PREFIX_OPERATOR) @ line 218
        $operatorSeed = 'SOAACAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIATA';

        $signer = new NkeySeedSigner($operatorSeed);

        self::assertStringStartsWith('O', $signer->publicKey());
    }

    /**
     * isValidPublicPrefix() match arm for PREFIX_SERVER. Removing it rejects a valid
     * server seed; the real code accepts it and the public key starts with 'N'.
     */
    public function testServerPrefixSeedIsAccepted(): void
    {
        // kills MatchArmRemoval(PREFIX_SERVER) @ line 218
        $serverSeed = 'SNAACAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIDHU';

        $signer = new NkeySeedSigner($serverSeed);

        self::assertStringStartsWith('N', $signer->publicKey());
    }

    /**
     * isValidPublicPrefix() match arm for PREFIX_CLUSTER. Removing it rejects a valid
     * cluster seed; the real code accepts it and the public key starts with 'C'.
     */
    public function testClusterPrefixSeedIsAccepted(): void
    {
        // kills MatchArmRemoval(PREFIX_CLUSTER) @ line 218
        $clusterSeed = 'SCAACAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAKOFQ';

        $signer = new NkeySeedSigner($clusterSeed);

        self::assertStringStartsWith('C', $signer->publicKey());
    }

    /**
     * isValidPublicPrefix() match arm for PREFIX_ACCOUNT (= 0). Removing it rejects a
     * valid account seed; the real code accepts it and the public key starts with 'A'.
     */
    public function testAccountPrefixSeedIsAccepted(): void
    {
        // kills MatchArmRemoval(PREFIX_ACCOUNT) @ line 218
        $accountSeed = 'SAAACAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAKM5I';

        $signer = new NkeySeedSigner($accountSeed);

        self::assertStringStartsWith('A', $signer->publicKey());
    }
}
