<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Auth\NkeySeedSigner;
use IDCT\NATS\Exception\NatsException;
use PHPUnit\Framework\TestCase;

final class NkeySeedSignerTest extends TestCase
{
    private const SAMPLE_SEED = 'SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY';
    private const SAMPLE_PUBLIC = 'UDXU4RCSJNZOIQHZNWXHXORDPRTGNJAHAHFRGZNEEJCPQTT2M7NLCNF4';

    public function testPublicKeyMatchesKnownUserSeed(): void
    {
        if (!function_exists('sodium_crypto_sign_seed_keypair')) {
            self::markTestSkipped('sodium extension is required for NkeySeedSigner tests.');
        }

        $signer = new NkeySeedSigner(self::SAMPLE_SEED);

        self::assertSame(self::SAMPLE_PUBLIC, $signer->publicKey());
    }

    public function testSignProducesVerifiableBase64UrlSignature(): void
    {
        if (!function_exists('sodium_crypto_sign_seed_keypair')) {
            self::markTestSkipped('sodium extension is required for NkeySeedSigner tests.');
        }

        $signer = new NkeySeedSigner(self::SAMPLE_SEED);
        $nonce = 'nonce-for-test';
        $signature = $signer->sign($nonce);

        self::assertMatchesRegularExpression('/^[A-Za-z0-9_-]+$/', $signature);

        $publicKeyRaw = $this->decodePublicKey(self::SAMPLE_PUBLIC);
        $signatureRaw = base64_decode(strtr($signature, '-_', '+/') . str_repeat('=', (4 - strlen($signature) % 4) % 4), true);

        self::assertIsString($signatureRaw);
        if ($signatureRaw === '') {
            self::fail('Expected a non-empty raw signature.');
        }
        self::assertTrue(sodium_crypto_sign_verify_detached($signatureRaw, $nonce, $publicKeyRaw));
    }

    public function testInvalidSeedChecksumIsRejected(): void
    {
        if (!function_exists('sodium_crypto_sign_seed_keypair')) {
            self::markTestSkipped('sodium extension is required for NkeySeedSigner tests.');
        }

        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('checksum');

        new NkeySeedSigner(substr(self::SAMPLE_SEED, 0, -1) . 'A');
    }

    /**
     * @return non-empty-string
     */
    private function decodePublicKey(string $encoded): string
    {
        $raw = $this->base32Decode($encoded);
        self::assertGreaterThanOrEqual(35, strlen($raw));

        $payload = substr($raw, 0, -2);
        $checksum = unpack('vchecksum', substr($raw, -2));
        self::assertIsArray($checksum);
        self::assertSame($this->crc16($payload), $checksum['checksum'] ?? null);

        self::assertSame(20 << 3, ord($payload[0]) & 248);

        $decoded = substr($payload, 1);
        if ($decoded === '') {
            self::fail('Expected decoded public key payload to be non-empty.');
        }

        return $decoded;
    }

    private function base32Decode(string $encoded): string
    {
        $alphabet = array_flip(str_split('ABCDEFGHIJKLMNOPQRSTUVWXYZ234567'));
        $buffer = 0;
        $bits = 0;
        $decoded = '';

        $length = strlen($encoded);
        for ($i = 0; $i < $length; $i++) {
            $char = $encoded[$i];
            self::assertArrayHasKey($char, $alphabet);

            $buffer = ($buffer << 5) | $alphabet[$char];
            $bits += 5;

            while ($bits >= 8) {
                $bits -= 8;
                $decoded .= chr(($buffer >> $bits) & 255);
            }
        }

        return $decoded;
    }

    private function crc16(string $data): int
    {
        $crc = 0;
        $length = strlen($data);

        for ($i = 0; $i < $length; $i++) {
            $crc ^= ord($data[$i]) << 8;

            for ($bit = 0; $bit < 8; $bit++) {
                if (($crc & 0x8000) !== 0) {
                    $crc = (($crc << 1) ^ 0x1021) & 0xffff;
                } else {
                    $crc = ($crc << 1) & 0xffff;
                }
            }
        }

        return $crc;
    }

    /**
     * A base32 string that decodes to only 3 bytes (passes base32Decode but fails the
     * >= 4 bytes check inside decode(), triggering "Invalid NKey encoding").
     */
    public function testTooShortBase32EncodingThrowsInvalidNKeyEncoding(): void
    {
        if (!function_exists('sodium_crypto_sign_seed_keypair')) {
            self::markTestSkipped('sodium extension is required for NkeySeedSigner tests.');
        }

        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('Invalid NKey encoding');

        // 'AAAAA' base32-decodes to 3 zero-bytes (trailing zero bit is valid),
        // which is fewer than the 4 bytes decode() requires before CRC stripping.
        new NkeySeedSigner('AAAAA');
    }

    /**
     * A single base32 character 'B' has 5 bits remaining after decoding no full bytes,
     * and those trailing bits are non-zero (B = 1), triggering "Invalid trailing bits".
     */
    public function testNonZeroTrailingBitsThrowsInvalidTrailingBits(): void
    {
        if (!function_exists('sodium_crypto_sign_seed_keypair')) {
            self::markTestSkipped('sodium extension is required for NkeySeedSigner tests.');
        }

        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('Invalid trailing bits in NKey encoding');

        new NkeySeedSigner('B');
    }

    /**
     * A seed string containing '1' (not in the base32 alphabet A-Z2-7) triggers
     * "Invalid base32 character in NKey encoding".
     */
    public function testInvalidBase32CharacterThrowsException(): void
    {
        if (!function_exists('sodium_crypto_sign_seed_keypair')) {
            self::markTestSkipped('sodium extension is required for NkeySeedSigner tests.');
        }

        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('Invalid base32 character in NKey encoding');

        // '1' is not a valid base32 character (alphabet is A-Z + 2-7).
        new NkeySeedSigner('SUAACAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCA1BAEQ');
    }

    /**
     * A correctly CRC-checked seed whose base32-decoded payload is only 3 bytes
     * (< 34) passes the inner decode() check but fails the decodeSeed() length guard,
     * triggering "Invalid NKey seed encoding".
     */
    public function testSeedTooShortForDecodeSeedThrowsInvalidNKeySeedEncoding(): void
    {
        if (!function_exists('sodium_crypto_sign_seed_keypair')) {
            self::markTestSkipped('sodium extension is required for NkeySeedSigner tests.');
        }

        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('Invalid NKey seed encoding');

        // KNKUCMNO base32-decodes to 5 bytes; after CRC stripping the payload is
        // 3 bytes, well below the 34-byte minimum decodeSeed() requires.
        new NkeySeedSigner('KNKUCMNO');
    }

    /**
     * A 36-byte seed whose first byte has b1 = 0 (not PREFIX_SEED = 144) triggers
     * "Invalid NKey seed prefix".
     */
    public function testWrongSeedPrefixB1ThrowsInvalidNKeySeedPrefix(): void
    {
        if (!function_exists('sodium_crypto_sign_seed_keypair')) {
            self::markTestSkipped('sodium extension is required for NkeySeedSigner tests.');
        }

        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('Invalid NKey seed prefix');

        // AAAACAIB... has b1 = 0 instead of the required PREFIX_SEED (144).
        new NkeySeedSigner('AAAACAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAN7AI');
    }

    /**
     * A seed with the correct b1 = PREFIX_SEED (144) but an invalid public prefix
     * byte (b2 = 255, which is not any of the recognised prefix values) triggers
     * "Invalid NKey seed prefix" via isValidPublicPrefix().
     */
    public function testInvalidPublicPrefixInSeedThrowsInvalidNKeySeedPrefix(): void
    {
        if (!function_exists('sodium_crypto_sign_seed_keypair')) {
            self::markTestSkipped('sodium extension is required for NkeySeedSigner tests.');
        }

        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('Invalid NKey seed prefix');

        // S74ACAIB... has b1 = 144 (correct) but b2 = 255 (no valid NKey type).
        new NkeySeedSigner('S74ACAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAPLMA');
    }

    /**
     * A seed with correct prefixes but a 33-byte inner seed (instead of 32) triggers
     * "Invalid NKey seed length".
     */
    public function testSeedInnerPayloadWrongLengthThrowsInvalidNKeySeedLength(): void
    {
        if (!function_exists('sodium_crypto_sign_seed_keypair')) {
            self::markTestSkipped('sodium extension is required for NkeySeedSigner tests.');
        }

        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('Invalid NKey seed length');

        // SUAACAIB...AIBQXVQ has valid prefixes (b1=144, b2=160 USER) but
        // the inner seed payload is 33 bytes, not the required 32.
        new NkeySeedSigner('SUAACAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBQXVQ');
    }

    /**
     * Verify that a synthetically constructed user seed (all-0x01 entropy bytes)
     * is accepted and produces a deterministic public key.
     * This exercises the happy path with a seed different from SAMPLE_SEED,
     * incidentally covering base32Encode's trailing-bits branch for
     * the 33-byte public-key payload (33 bytes = 264 bits, 264 % 5 = 4 leftover bits).
     */
    public function testSyntheticUserSeedIsAccepted(): void
    {
        if (!function_exists('sodium_crypto_sign_seed_keypair')) {
            self::markTestSkipped('sodium extension is required for NkeySeedSigner tests.');
        }

        // SUAACAIB... is a valid user seed with 32 bytes of 0x01 as the raw entropy.
        $signer = new NkeySeedSigner('SUAACAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAN7EY');
        $publicKey = $signer->publicKey();

        // Must start with 'U' (user prefix character in NKey encoding).
        self::assertStringStartsWith('U', $publicKey);

        // Must be non-empty and match the base32 alphabet only.
        self::assertMatchesRegularExpression('/^[A-Z2-7]+$/', $publicKey);

        // sign() must succeed and return a base64url string.
        $signature = $signer->sign('test-nonce');
        self::assertMatchesRegularExpression('/^[A-Za-z0-9_-]+$/', $signature);
    }

    /**
     * Verify that a synthetically constructed account seed is accepted and produces
     * a public key starting with 'A' (PREFIX_ACCOUNT = 0 in NKey encoding).
     */
    public function testSyntheticAccountSeedIsAccepted(): void
    {
        if (!function_exists('sodium_crypto_sign_seed_keypair')) {
            self::markTestSkipped('sodium extension is required for NkeySeedSigner tests.');
        }

        // SAAACAIB... is a valid account seed with 32 bytes of 0x01 as raw entropy.
        $signer = new NkeySeedSigner('SAAACAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAKM5I');
        $publicKey = $signer->publicKey();

        // Must start with 'A' (account prefix character).
        self::assertStringStartsWith('A', $publicKey);
        self::assertMatchesRegularExpression('/^[A-Z2-7]+$/', $publicKey);
    }
}
