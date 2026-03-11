<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Protocol\ProtocolFrameType;
use IDCT\NATS\Protocol\ProtocolParser;
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

    /**
     * Verifies malformed MSG/HMSG lines are rejected.
     */
    public function testRejectsMalformedMessageLines(): void
    {
        $cases = [
            "MSG only-two-fields\r\n",
            "MSG s 1 5 6 7\r\n",
            "HMSG too short\r\n",
            "HMSG s 1 2 3 4 5 6\r\n",
        ];

        foreach ($cases as $frameLine) {
            $parser = new ProtocolParser();

            try {
                $parser->push($frameLine);
                self::fail('Expected malformed frame to throw: ' . $frameLine);
            } catch (ProtocolException) {
                self::assertTrue(true);
            }
        }
    }

    /**
     * Verifies parser rejects MSG/HMSG payloads without trailing CRLF.
     */
    public function testRejectsMessagePayloadWithoutTerminatingCrLf(): void
    {
        $parser = new ProtocolParser();

        $this->expectException(ProtocolException::class);
        $parser->push("MSG s 1 5\r\nhelloXX");
    }

    /**
     * Verifies parser can reconstruct MSG frames for many fragmentation patterns.
     */
    public function testPropertyStyleFragmentedMsgReassembly(): void
    {
        $wire = "MSG updates 17 11\r\nhello world\r\n";

        foreach ($this->fragmentVariants($wire) as $chunks) {
            $parser = new ProtocolParser();
            $frames = [];

            foreach ($chunks as $chunk) {
                $frames = array_merge($frames, $parser->push($chunk));
            }

            self::assertCount(1, $frames);
            self::assertSame(ProtocolFrameType::Msg, $frames[0]->type);
            self::assertSame('updates', $frames[0]->subject);
            self::assertSame(17, $frames[0]->sid);
            self::assertSame('hello world', $frames[0]->payload);
        }
    }

    /**
     * Verifies parser can reconstruct HMSG frames for many fragmentation patterns.
     */
    public function testPropertyStyleFragmentedHmsgReassembly(): void
    {
        $headersAndPayload = "NATS/1.0\r\nStatus:100\r\n\r\nok";
        $wire = "HMSG hb 3 24 26\r\n" . $headersAndPayload . "\r\n";

        foreach ($this->fragmentVariants($wire) as $chunks) {
            $parser = new ProtocolParser();
            $frames = [];

            foreach ($chunks as $chunk) {
                $frames = array_merge($frames, $parser->push($chunk));
            }

            self::assertCount(1, $frames);
            self::assertSame(ProtocolFrameType::HMsg, $frames[0]->type);
            self::assertSame('hb', $frames[0]->subject);
            self::assertSame(3, $frames[0]->sid);
            self::assertSame(24, $frames[0]->headerBytes);
            self::assertSame(26, $frames[0]->totalBytes);
            self::assertSame($headersAndPayload, $frames[0]->payload);
        }
    }

    /**
     * Builds deterministic chunking variants for property-style parser validation.
     *
     * @return list<list<string>>
     */
    private function fragmentVariants(string $wire): array
    {
        $length = strlen($wire);

        return [
            [$wire],
            str_split($wire, 1),
            $this->splitByPattern($wire, [2, 3, 5, 1, 4]),
            $this->splitByPattern($wire, [7, 2, 9]),
            $this->splitByPattern($wire, [$length - 1, 1]),
        ];
    }

    /**
     * Splits payload into chunks using repeating sizes.
     *
     * @param list<int> $pattern
     * @return list<string>
     */
    private function splitByPattern(string $wire, array $pattern): array
    {
        $chunks = [];
        $offset = 0;
        $index = 0;
        $length = strlen($wire);

        while ($offset < $length) {
            $size = max(1, $pattern[$index % count($pattern)]);
            $chunks[] = substr($wire, $offset, $size);
            $offset += $size;
            $index++;
        }

        return $chunks;
    }
}
