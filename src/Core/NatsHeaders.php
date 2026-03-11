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
}
