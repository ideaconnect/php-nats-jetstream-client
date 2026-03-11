<?php

declare(strict_types=1);

namespace Idct\Nats\Core;

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
        if ($lines === false || $lines === []) {
            return [];
        }

        // First line is protocol version (for example "NATS/1.0").
        array_shift($lines);

        $headers = [];
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
