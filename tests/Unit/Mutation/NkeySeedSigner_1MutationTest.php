<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Auth\NkeySeedSigner;
use IDCT\NATS\Exception\NatsException;
use PHPUnit\Framework\TestCase;

/**
 * Second mutation-killing chunk for NkeySeedSigner.
 *
 * Pins two behaviours the first chunk left uncovered:
 *  - the CURVE public-prefix match arm, and
 *  - the exact 0xF8 (248) mask applied to the second seed prefix byte.
 *
 * Seeds are synthesised with correct CRC16 checksums (32 bytes of 0x01 entropy)
 * so the real code accepts/rejects them deterministically.
 */
final class NkeySeedSigner_1MutationTest extends TestCase
{
    protected function setUp(): void
    {
        if (!function_exists('sodium_crypto_sign_seed_keypair')) {
            self::markTestSkipped('sodium extension is required for NkeySeedSigner tests.');
        }
    }

    /**
     * isValidPublicPrefix() match arm for PREFIX_CURVE (= 23 << 3 = 184).
     * Removing the CURVE arm makes a valid curve seed fall through to
     * default => false, so the constructor throws "Invalid NKey seed prefix".
     * The real code accepts it and the derived public key starts with 'X'
     * (chr(184) top-5-bits = 0b10111 = 23 = alphabet index of 'X').
     *
     * Kills: MatchArmRemoval(PREFIX_CURVE) @ line 218.
     */
    public function testCurvePrefixSeedIsAccepted(): void
    {
        $curveSeed = 'SXAACAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAN4QM';

        $signer = new NkeySeedSigner($curveSeed);

        self::assertStringStartsWith('X', $signer->publicKey());
        self::assertMatchesRegularExpression('/^[A-Z2-7]+$/', $signer->publicKey());
    }

    /**
     * decodeSeed(): $b2 = ... | ((ord($raw[1]) & 248) >> 3). Bit 3 (value 8) of the
     * mask maps onto bit 0 of $b2. This seed has raw[1] = 136 (0b10001000, bit 3 set),
     * so the real mask (248) yields b2 = 17 (odd, not a valid NKey prefix) and the
     * constructor throws "Invalid NKey seed prefix".
     *
     * The DecrementInteger mutant (mask 247 = 0b11110111) clears bit 3, yielding
     * b2 = 16 (CLUSTER, valid); it would accept the seed and derive a cluster key,
     * so it never throws "Invalid NKey seed prefix".
     *
     * Kills: DecrementInteger(248 -> 247) @ line 83.
     */
    public function testSeedSecondPrefixByteMaskRetainsBitThree(): void
    {
        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('Invalid NKey seed prefix');

        // raw[0]=144 (PREFIX_SEED), raw[1]=136 -> real b2=17 (invalid), mask-247 b2=16 (CLUSTER).
        new NkeySeedSigner('SCEACAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAJKAI');
    }
}
