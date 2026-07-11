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
     * A value may be a single string (one header line) or a list of strings (one line per value, for
     * multi-value/multimap headers per ADR-4) - so `['Link' => ['a', 'b']]` emits two `Link:` lines.
     *
     * @param array<string,string|list<string>> $headers
     */
    public static function toWireBlock(array $headers): string
    {
        $lines = ['NATS/1.0'];

        foreach ($headers as $name => $value) {
            $name = (string) $name;
            // A header name must be a non-empty token: an empty/blank name emits ":value" (dropped on
            // round-trip), and a name containing whitespace or a colon corrupts the "key:value" parse.
            if ($name === '' || preg_match('/[\s:]/', $name) === 1) {
                throw new \InvalidArgumentException('Header name must be a non-empty token without whitespace or a colon');
            }

            // A list value emits one line per element (multimap); a scalar emits a single line.
            $values = is_array($value) ? $value : [$value];
            foreach ($values as $singleValue) {
                $singleValue = (string) $singleValue;
                if (preg_match('/[\r\n]/', $singleValue) === 1) {
                    throw new \InvalidArgumentException('Header values must not contain CR or LF characters');
                }

                // Compact "key:value" form (no space after the colon) because some server-side header
                // parsers do not trim a leading space from values. Surrounding whitespace in a value is
                // not significant - it is trimmed here and again on decode - so values round-trip
                // symmetrically rather than asymmetrically losing leading/trailing spaces.
                $lines[] = $name . ':' . trim($singleValue);
            }
        }

        // NATS headers terminate with an additional CRLF after all header lines.
        return implode("\r\n", $lines) . "\r\n\r\n";
    }

    /**
     * Decodes a NATS/1.0 wire header block as a multimap, preserving every value of a repeated header
     * name (ADR-4). Use this when a header may legitimately appear more than once; the system headers
     * the client reads are single-valued, so {@see fromWireBlock()} (last-value-wins) remains the
     * convenient default.
     *
     * @return array<string,list<string>>
     */
    public static function fromWireBlockMulti(?string $rawHeaders): array
    {
        if ($rawHeaders === null || $rawHeaders === '') {
            return [];
        }

        // explode() on the literal CRLF separator produces the exact same pieces (including empties)
        // as the previous preg_split('/\r\n/') at a fraction of the cost - this runs on the
        // per-delivery hot path (#139).
        $lines = explode("\r\n", $rawHeaders);

        $headers = [];
        $firstLine = array_shift($lines);
        if (preg_match('/^NATS\/1\.0\s+(\d{3})(?:\s+(.*))?$/', $firstLine, $matches) === 1) {
            $headers['Status'] = [$matches[1]];
            $description = trim((string) ($matches[2] ?? ''));
            if ($description !== '') {
                $headers['Description'] = [$description];
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
            if ($name === '') {
                continue;
            }

            $headers[$name][] = trim(substr($line, $separator + 1));
        }

        return $headers;
    }

    /**
     * Decodes a NATS/1.0 wire header block into a name/value map.
     *
     * A repeated header name collapses to last-value-wins (the map is `array<string,string>`, not a
     * multimap); use {@see fromWireBlockMulti()} to preserve every value. This matches every header the
     * client consumes - the system headers it reads (Status, Nats-Sequence, KV-Operation,
     * Nats-Consumer-Stalled, ...) are single-valued.
     *
     * @return array<string,string>
     */
    public static function fromWireBlock(?string $rawHeaders): array
    {
        if ($rawHeaders === null || $rawHeaders === '') {
            return [];
        }

        // explode() on the literal CRLF separator produces the exact same pieces (including empties)
        // as the previous preg_split('/\r\n/') at a fraction of the cost - this runs on the
        // per-delivery hot path (#139).
        $lines = explode("\r\n", $rawHeaders);

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
