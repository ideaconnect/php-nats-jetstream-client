<?php

declare(strict_types=1);

namespace Idct\Nats\Tests\Unit;

use Idct\Nats\Exception\ProtocolException;
use Idct\Nats\Protocol\ProtocolFrameType;
use Idct\Nats\Protocol\ProtocolParser;
use PHPUnit\Framework\TestCase;

final class ProtocolParserTest extends TestCase
{
    /**
     * Verifies parser recognizes line-based control frames in sequence.
     */
    public function testParsesControlFrames(): void
    {
        $parser = new ProtocolParser();
        $frames = $parser->push("PING\r\nPONG\r\n+OK\r\n-ERR 'boom'\r\n");

        self::assertCount(4, $frames);
        self::assertSame(ProtocolFrameType::Ping, $frames[0]->type);
        self::assertSame(ProtocolFrameType::Pong, $frames[1]->type);
        self::assertSame(ProtocolFrameType::Ok, $frames[2]->type);
        self::assertSame(ProtocolFrameType::Err, $frames[3]->type);
        self::assertSame("'boom'", $frames[3]->error);
    }

    /**
     * Verifies parser reassembles MSG payload from fragmented chunks.
     */
    public function testParsesFragmentedMsgFrame(): void
    {
        $parser = new ProtocolParser();

        $framesA = $parser->push("MSG updates 17 5\r\nhe");
        $framesB = $parser->push("llo\r\n");

        self::assertCount(0, $framesA);
        self::assertCount(1, $framesB);
        self::assertSame(ProtocolFrameType::Msg, $framesB[0]->type);
        self::assertSame('updates', $framesB[0]->subject);
        self::assertSame(17, $framesB[0]->sid);
        self::assertSame('hello', $framesB[0]->payload);
    }

    /**
     * Verifies parser extracts HMSG metadata and combined payload bytes.
     */
    public function testParsesHmsgFrame(): void
    {
        $parser = new ProtocolParser();
        $headersAndPayload = "NATS/1.0\r\n\r\nhello";

        $frames = $parser->push("HMSG orders 10 12 17\r\n{$headersAndPayload}\r\n");

        self::assertCount(1, $frames);
        self::assertSame(ProtocolFrameType::HMsg, $frames[0]->type);
        self::assertSame('orders', $frames[0]->subject);
        self::assertSame(10, $frames[0]->sid);
        self::assertSame(12, $frames[0]->headerBytes);
        self::assertSame(17, $frames[0]->totalBytes);
        self::assertSame($headersAndPayload, $frames[0]->payload);
    }

    /**
     * Verifies parser rejects unsupported frame commands.
     */
    public function testThrowsForUnsupportedFrame(): void
    {
        $parser = new ProtocolParser();

        $this->expectException(ProtocolException::class);
        $parser->push("WAT something\r\n");
    }
}
