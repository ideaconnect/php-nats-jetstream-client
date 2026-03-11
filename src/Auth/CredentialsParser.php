<?php

declare(strict_types=1);

namespace IDCT\NATS\Auth;

use IDCT\NATS\Exception\NatsException;

/**
 * Parses NATS `.creds` credential files containing JWT and NKey seed pairs.
 */
final class CredentialsParser
{
    /**
     * Extracts JWT and NKey seed from a `.creds` file path.
     *
     * @return array{jwt: string, nkeySeed: string}
     */
    public static function fromFile(string $path): array
    {
        if (!is_file($path) || !is_readable($path)) {
            throw new NatsException('Credentials file not found or not readable: ' . $path);
        }

        $contents = file_get_contents($path);
        if ($contents === false) {
            throw new NatsException('Failed to read credentials file: ' . $path);
        }

        return self::parse($contents);
    }

    /**
     * Extracts JWT and NKey seed from credential file contents.
     *
     * NATS `.creds` files contain two PEM-like blocks:
     * - `-----BEGIN NATS USER JWT-----` / `-----END NATS USER JWT-----`
     * - `-----BEGIN USER NKEY SEED-----` / `-----END USER NKEY SEED-----`
     *
     * @return array{jwt: string, nkeySeed: string}
     */
    public static function parse(string $contents): array
    {
        $jwt = self::extractBlock($contents, 'NATS USER JWT');
        if ($jwt === null) {
            throw new NatsException('Credentials file does not contain a NATS USER JWT block');
        }

        $nkeySeed = self::extractBlock($contents, 'USER NKEY SEED');
        if ($nkeySeed === null) {
            throw new NatsException('Credentials file does not contain a USER NKEY SEED block');
        }

        return ['jwt' => $jwt, 'nkeySeed' => $nkeySeed];
    }

    /**
     * Extracts the content between BEGIN/END markers of the given block type.
     */
    private static function extractBlock(string $contents, string $blockType): ?string
    {
        $pattern = '/-----BEGIN ' . preg_quote($blockType, '/') . '-----\s*\n(.+?)\n\s*-----END ' . preg_quote($blockType, '/') . '-----/s';
        if (preg_match($pattern, $contents, $matches) !== 1) {
            return null;
        }

        return trim($matches[1]);
    }
}
