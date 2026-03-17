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
}