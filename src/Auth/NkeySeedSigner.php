<?php

declare(strict_types=1);

namespace IDCT\NATS\Auth;

use IDCT\NATS\Exception\NatsException;

/**
 * Signs NATS server nonces using an encoded NKey seed.
 */
final class NkeySeedSigner implements NonceSignerInterface
{
    private const PREFIX_SEED = 18 << 3;
    private const PREFIX_SERVER = 13 << 3;
    private const PREFIX_CLUSTER = 2 << 3;
    private const PREFIX_OPERATOR = 14 << 3;
    private const PREFIX_ACCOUNT = 0;
    private const PREFIX_USER = 20 << 3;
    private const PREFIX_CURVE = 23 << 3;
    private const BASE32_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';

    private readonly string $secretKey;
    private readonly string $publicKeyRaw;
    private readonly int $publicPrefix;

    public function __construct(string $seed)
    {
        if (!function_exists('sodium_crypto_sign_seed_keypair')) {
            throw new NatsException('NkeySeedSigner requires the sodium extension');
        }

        [$this->publicPrefix, $rawSeed] = self::decodeSeed($seed);
        if ($rawSeed === '') {
            throw new NatsException('Invalid NKey seed length');
        }

        try {
            $keyPair = sodium_crypto_sign_seed_keypair($rawSeed);
            $this->secretKey = sodium_crypto_sign_secretkey($keyPair);
            $this->publicKeyRaw = sodium_crypto_sign_publickey($keyPair);
        } catch (\SodiumException $e) {
            throw new NatsException('Failed to derive Ed25519 key pair from NKey seed', 0, $e);
        }
    }

    /**
     * Returns the public NKey associated with the configured seed.
     */
    public function publicKey(): string
    {
        return self::encodePublicKey($this->publicPrefix, $this->publicKeyRaw);
    }

    /**
     * Signs the server nonce using Ed25519 and returns a base64url signature.
     */
    public function sign(string $nonce): string
    {
        try {
            return self::base64UrlEncode(sodium_crypto_sign_detached($nonce, $this->secretKey));
        } catch (\SodiumException $e) {
            throw new NatsException('Failed to sign server nonce', 0, $e);
        }
    }

    /**
     * @return array{0: int, 1: string}
     */
    private static function decodeSeed(string $seed): array
    {
        $raw = self::decode(trim(strtoupper($seed)));
        if (strlen($raw) < 34) {
            throw new NatsException('Invalid NKey seed encoding');
        }

        $b1 = ord($raw[0]) & 248;
        $b2 = ((ord($raw[0]) & 7) << 5) | ((ord($raw[1]) & 248) >> 3);

        if ($b1 !== self::PREFIX_SEED || !self::isValidPublicPrefix($b2)) {
            throw new NatsException('Invalid NKey seed prefix');
        }

        $payload = substr($raw, 2);
        if (strlen($payload) !== 32) {
            throw new NatsException('Invalid NKey seed length');
        }

        return [$b2, $payload];
    }

    private static function encodePublicKey(int $publicPrefix, string $publicKeyRaw): string
    {
        if (!self::isValidPublicPrefix($publicPrefix)) {
            throw new NatsException('Invalid NKey public prefix');
        }

        if ($publicPrefix < 0 || $publicPrefix > 255) {
            throw new NatsException('Invalid NKey public prefix width');
        }

        if (strlen($publicKeyRaw) !== 32) {
            throw new NatsException('Invalid Ed25519 public key length');
        }

        $payload = chr($publicPrefix) . $publicKeyRaw;

        return self::base32Encode($payload . pack('v', self::crc16($payload)));
    }

    private static function decode(string $encoded): string
    {
        $raw = self::base32Decode($encoded);
        if (strlen($raw) < 4) {
            throw new NatsException('Invalid NKey encoding');
        }

        $payload = substr($raw, 0, -2);
        $checksum = unpack('vchecksum', substr($raw, -2));
        if (!is_array($checksum) || !isset($checksum['checksum'])) {
            throw new NatsException('Failed to decode NKey checksum');
        }

        if (self::crc16($payload) !== $checksum['checksum']) {
            throw new NatsException('Invalid NKey checksum');
        }

        return $payload;
    }

    private static function base32Encode(string $bytes): string
    {
        $alphabet = self::BASE32_ALPHABET;
        $buffer = 0;
        $bits = 0;
        $encoded = '';

        $length = strlen($bytes);
        for ($i = 0; $i < $length; $i++) {
            $buffer = ($buffer << 8) | ord($bytes[$i]);
            $bits += 8;

            while ($bits >= 5) {
                $bits -= 5;
                $encoded .= $alphabet[($buffer >> $bits) & 31];
            }
        }

        if ($bits > 0) {
            $encoded .= $alphabet[($buffer << (5 - $bits)) & 31];
        }

        return $encoded;
    }

    private static function base32Decode(string $encoded): string
    {
        $alphabet = array_flip(str_split(self::BASE32_ALPHABET));
        $buffer = 0;
        $bits = 0;
        $decoded = '';

        $length = strlen($encoded);
        for ($i = 0; $i < $length; $i++) {
            $char = $encoded[$i];
            if (!isset($alphabet[$char])) {
                throw new NatsException('Invalid base32 character in NKey encoding');
            }

            $buffer = ($buffer << 5) | $alphabet[$char];
            $bits += 5;

            while ($bits >= 8) {
                $bits -= 8;
                $decoded .= chr(($buffer >> $bits) & 255);
            }
        }

        if ($bits > 0 && (($buffer & ((1 << $bits) - 1)) !== 0)) {
            throw new NatsException('Invalid trailing bits in NKey encoding');
        }

        return $decoded;
    }

    private static function crc16(string $data): int
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

    private static function base64UrlEncode(string $bytes): string
    {
        return rtrim(strtr(base64_encode($bytes), '+/', '-_'), '=');
    }

    private static function isValidPublicPrefix(int $prefix): bool
    {
        return match ($prefix) {
            self::PREFIX_OPERATOR,
            self::PREFIX_SERVER,
            self::PREFIX_CLUSTER,
            self::PREFIX_ACCOUNT,
            self::PREFIX_USER,
            self::PREFIX_CURVE => true,
            default => false,
        };
    }
}