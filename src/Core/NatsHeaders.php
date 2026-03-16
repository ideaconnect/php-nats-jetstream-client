<?php

declare(strict_types=1);

namespace IDCT\NATS\Core;

/**
 * Utilities for parsing and serializing NATS header blocks.
 */
final class NatsHeaders
{
    /**
     * Encodes headers using the NATS/1.0 header block wire format.
     *
     * @param array<string,string> $headers
     */
    public static function toWireBlock(array $headers): string
    {
        $lines = ['NATS/1.0'];

        foreach ($headers as $name => $value) {
            $name = (string) $name;
            if (preg_match('/[\r\n]/', $name) || preg_match('/[\r\n]/', $value)) {
                throw new \InvalidArgumentException('Header names and values must not contain CR or LF characters');
            }

            // Use compact "key:value" form because some server-side header parsers
            // do not trim leading spaces from values.
            $lines[] = $name . ':' . $value;
        }

        // NATS headers terminate with an additional CRLF after all header lines.
        return implode("\r\n", $lines) . "\r\n\r\n";
    }

    /**
     * Decodes a NATS/1.0 wire header block into a name/value map.
     *
     * @return array<string,string>
     */
    public static function fromWireBlock(?string $rawHeaders): array
    {
        if ($rawHeaders === null || $rawHeaders === '') {
            return [];
        }

        $lines = preg_split('/\r\n/', $rawHeaders);
        if ($lines === false) {
            return [];
        }

        $headers = [];
        // First line may be either "NATS/1.0" or "NATS/1.0 <status> <description>".
        $firstLine = array_shift($lines);
        if (preg_match('/^NATS\/1\.0\s+(\d{3})(?:\s+(.*))?$/', $firstLine, $matches) === 1) {
            $headers['Status'] = $matches[1];
            $description = trim((string) ($matches[2] ?? ''));
            if ($description !== '') {
                $headers['Description'] = $description;
            }
        }

        foreach ($lines as $line) {
            if ($line === '') {
                break;
            }

            $separator = strpos($line, ':');
            if ($separator === false) {
                continue;
            }

            $name = trim(substr($line, 0, $separator));
            $value = trim(substr($line, $separator + 1));
            if ($name === '') {
                continue;
            }

            $headers[$name] = $value;
        }

        return $headers;
    }
}
