<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Core\NatsHeaders;
use PHPUnit\Framework\TestCase;

final class NatsHeadersTest extends TestCase
{
    public function testRoundTripWireEncoding(): void
    {
        $raw = NatsHeaders::toWireBlock([
            'Nats-Msg-Id' => '123',
            'X-Trace' => 'abc',
        ]);

        self::assertStringStartsWith("NATS/1.0\r\n", $raw);
        self::assertStringEndsWith("\r\n\r\n", $raw);
        self::assertSame([
            'Nats-Msg-Id' => '123',
            'X-Trace' => 'abc',
        ], NatsHeaders::fromWireBlock($raw));
    }

    public function testFromWireBlockSkipsMalformedHeaderLines(): void
    {
        self::assertSame([], NatsHeaders::fromWireBlock(null));
        self::assertSame([], NatsHeaders::fromWireBlock(''));

        $raw = "NATS/1.0\r\n" .
            "NoSeparator\r\n" .
            ":missing-name\r\n" .
            "Valid: value\r\n" .
            "\r\n";

        self::assertSame(['Valid' => 'value'], NatsHeaders::fromWireBlock($raw));
    }

    public function testFromWireBlockParsesStatusLine(): void
    {
        $raw = "NATS/1.0 100 Idle Heartbeat\r\n" .
            "Nats-Consumer-Stalled: _INBOX.123\r\n" .
            "\r\n";

        self::assertSame([
            'Status' => '100',
            'Description' => 'Idle Heartbeat',
            'Nats-Consumer-Stalled' => '_INBOX.123',
        ], NatsHeaders::fromWireBlock($raw));
    }
}
