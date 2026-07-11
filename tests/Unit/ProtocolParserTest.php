<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Protocol\Enum\ProtocolFrameType;
use IDCT\NATS\Protocol\ProtocolParser;
use PHPUnit\Framework\TestCase;

final class ProtocolParserTest extends TestCase
{
    /**
     * Verifies parser recognizes line-based control frames in sequence.
     */
    public function testRejectsOverflowingSizeField(): void
    {
        $parser = new ProtocolParser();

        // A 20-digit size exceeds PHP_INT_MAX; (int) would silently saturate, so it must be rejected.
        $this->expectException(ProtocolException::class);
        $parser->push("MSG updates 1 99999999999999999999\r\n");
    }

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
     * Operation verbs are matched case-insensitively and may be separated from their arguments by any
     * whitespace (space or tab), aligning with the NATS wire spec. Real servers send upper-case verbs,
     * so this only adds leniency; case/whitespace of arguments and payloads is preserved.
     */
    public function testParsesVerbsCaseInsensitivelyAndWithTabSeparators(): void
    {
        $parser = new ProtocolParser();

        // Lower/mixed-case control verbs.
        $control = $parser->push("ping\r\nPong\r\n+ok\r\n-err Boom\r\ninfo {\"server_id\":\"S1\"}\r\n");
        self::assertSame(ProtocolFrameType::Ping, $control[0]->type);
        self::assertSame(ProtocolFrameType::Pong, $control[1]->type);
        self::assertSame(ProtocolFrameType::Ok, $control[2]->type);
        self::assertSame(ProtocolFrameType::Err, $control[3]->type);
        self::assertSame('Boom', $control[3]->error);
        self::assertSame(ProtocolFrameType::Info, $control[4]->type);
        self::assertSame('{"server_id":"S1"}', $control[4]->infoPayload);

        // Lower-case MSG verb.
        $msg = $parser->push("msg updates 1 5\r\nhello\r\n");
        self::assertSame(ProtocolFrameType::Msg, $msg[0]->type);
        self::assertSame('updates', $msg[0]->subject);
        self::assertSame('hello', $msg[0]->payload);

        // Tab-separated MSG fields with a mixed-case verb.
        $tabbed = $parser->push("Msg\tupdates\t2\t5\r\nworld\r\n");
        self::assertSame(ProtocolFrameType::Msg, $tabbed[0]->type);
        self::assertSame('updates', $tabbed[0]->subject);
        self::assertSame(2, $tabbed[0]->sid);
        self::assertSame('world', $tabbed[0]->payload);

        // Lower-case HMSG verb.
        $headers = "NATS/1.0\r\nX-A: 1\r\n\r\n";
        $hmsg = $parser->push(sprintf("hmsg orders 3 %d %d\r\n%shi\r\n", strlen($headers), strlen($headers) + 2, $headers));
        self::assertSame(ProtocolFrameType::HMsg, $hmsg[0]->type);
        self::assertSame('orders', $hmsg[0]->subject);
        self::assertSame($headers . 'hi', $hmsg[0]->payload);
    }

    /**
     * The fast-path tokenization (#140) must defer to the whitespace-tolerant split whenever a
     * control line is not canonically single-space separated: doubled spaces produce empty
     * explode() tokens, and a tab hidden inside a space-separated token changes the real token
     * count (turning a 4-token-looking MSG line into the 5-token reply-to form).
     */
    public function testParsesControlLinesWithMultiSpaceAndEmbeddedTabSeparators(): void
    {
        $parser = new ProtocolParser();

        // Multi-space separators between every field (documented server leniency).
        $frames = $parser->push("MSG  updates   7  5\r\nhello\r\n");
        self::assertCount(1, $frames);
        self::assertSame(ProtocolFrameType::Msg, $frames[0]->type);
        self::assertSame('updates', $frames[0]->subject);
        self::assertSame(7, $frames[0]->sid);
        self::assertNull($frames[0]->replyTo);
        self::assertSame('hello', $frames[0]->payload);

        // A tab between sid and reply-to keeps the space-split token count plausible (4) while the
        // real whitespace token count is 5 - the reply-to must still be recognized.
        $tabbed = $parser->push("MSG updates 9\t_INBOX.r 6\r\nworld!\r\n");
        self::assertCount(1, $tabbed);
        self::assertSame(ProtocolFrameType::Msg, $tabbed[0]->type);
        self::assertSame(9, $tabbed[0]->sid);
        self::assertSame('_INBOX.r', $tabbed[0]->replyTo);
        self::assertSame('world!', $tabbed[0]->payload);

        // HMSG with multi-space separators tokenizes identically.
        $headers = "NATS/1.0\r\n\r\n";
        $hmsg = $parser->push(sprintf(
            "HMSG  orders  4  %d  %d\r\n%shey\r\n",
            strlen($headers),
            strlen($headers) + 3,
            $headers,
        ));
        self::assertCount(1, $hmsg);
        self::assertSame(ProtocolFrameType::HMsg, $hmsg[0]->type);
        self::assertSame('orders', $hmsg[0]->subject);
        self::assertSame(4, $hmsg[0]->sid);
        self::assertSame($headers . 'hey', $hmsg[0]->payload);
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

    public function testParsesHmsgFrameWithReplyTo(): void
    {
        $parser = new ProtocolParser();
        $headersAndPayload = "NATS/1.0\r\n\r\nworld";

        $frames = $parser->push("HMSG orders 10 inbox.reply 12 17\r\n{$headersAndPayload}\r\n");

        self::assertCount(1, $frames);
        self::assertSame(ProtocolFrameType::HMsg, $frames[0]->type);
        self::assertSame('orders', $frames[0]->subject);
        self::assertSame(10, $frames[0]->sid);
        self::assertSame('inbox.reply', $frames[0]->replyTo);
        self::assertSame(12, $frames[0]->headerBytes);
        self::assertSame(17, $frames[0]->totalBytes);
        self::assertSame($headersAndPayload, $frames[0]->payload);
    }

    public function testBuffersPartialControlLineUntilCrLf(): void
    {
        $parser = new ProtocolParser();

        $framesA = $parser->push('PIN');
        $framesB = $parser->push("G\r\n");

        self::assertSame([], $framesA);
        self::assertCount(1, $framesB);
        self::assertSame(ProtocolFrameType::Ping, $framesB[0]->type);
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
     * @return iterable<string, array{string}>
     */
    public static function malformedMessageLineProvider(): iterable
    {
        yield 'MSG with only two fields' => ["MSG only-two-fields\r\n"];
        yield 'MSG with too many fields' => ["MSG s 1 5 6 7\r\n"];
        yield 'HMSG with too few fields' => ["HMSG too short\r\n"];
        yield 'HMSG with too many fields' => ["HMSG s 1 2 3 4 5 6\r\n"];
    }

    /**
     * Verifies malformed MSG/HMSG lines are rejected.
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('malformedMessageLineProvider')]
    public function testRejectsMalformedMessageLines(string $frameLine): void
    {
        $parser = new ProtocolParser();

        $this->expectException(ProtocolException::class);
        $parser->push($frameLine);
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

    /**
     * Verifies a large MSG payload containing embedded CRLF bytes reassembles correctly when fed
     * one byte at a time (exercises the pending-frame path that avoids re-scanning the buffer).
     */
    public function testParsesLargeFragmentedMsgWithEmbeddedCrlf(): void
    {
        $payload = str_repeat("ab\r\ncd", 1000); // 6000 bytes, with embedded CRLF sequences
        $wire = 'MSG big 7 ' . strlen($payload) . "\r\n" . $payload . "\r\n";

        $parser = new ProtocolParser();
        $frames = [];
        foreach (str_split($wire, 1) as $byte) {
            $frames = array_merge($frames, $parser->push($byte));
        }

        self::assertCount(1, $frames);
        self::assertSame(ProtocolFrameType::Msg, $frames[0]->type);
        self::assertSame('big', $frames[0]->subject);
        self::assertSame(7, $frames[0]->sid);
        self::assertSame($payload, $frames[0]->payload);
    }

    /**
     * Verifies a multi-hundred-KiB payload delivered in 8 KiB transport-sized reads reassembles
     * byte-identically, including bytes for a following frame arriving in the completing chunk
     * (exercises the pending chunk-list accumulation path, #140). The interleaved empty push
     * covers polling the parser while chunks are diverted to the accumulation list.
     */
    public function testParsesLargePayloadDeliveredInTransportSizedChunks(): void
    {
        // Embedded CRLFs ensure reassembly cannot rely on line scanning inside the payload.
        $payload = str_repeat("abcdef\r\nGH", 30000); // 300000 bytes
        $wire = 'MSG big 5 ' . strlen($payload) . "\r\n" . $payload . "\r\nPING\r\n";

        $parser = new ProtocolParser();
        $frames = [];
        foreach (str_split($wire, 8192) as $index => $chunk) {
            $frames = array_merge($frames, $parser->push($chunk));

            if ($index === 1) {
                // Mid-accumulation empty push: must emit nothing and must not disturb the
                // pending frame's buffered bytes.
                self::assertSame([], $parser->push(''));
            }
        }

        self::assertCount(2, $frames);
        self::assertSame(ProtocolFrameType::Msg, $frames[0]->type);
        self::assertSame('big', $frames[0]->subject);
        self::assertSame(5, $frames[0]->sid);
        self::assertSame($payload, $frames[0]->payload);
        self::assertSame(ProtocolFrameType::Ping, $frames[1]->type);
    }

    /**
     * Verifies trailing bytes after a completed fragmented frame remain buffered for the next frame.
     */
    public function testCompletedPendingFrameLeavesTrailingBytesForNextFrame(): void
    {
        $parser = new ProtocolParser();

        // First frame's payload is pending after the control line; the next push completes it
        // and also carries a following control frame.
        self::assertCount(0, $parser->push("MSG a 1 5\r\n"));
        $frames = $parser->push("hello\r\nPONG\r\n");

        self::assertCount(2, $frames);
        self::assertSame(ProtocolFrameType::Msg, $frames[0]->type);
        self::assertSame('hello', $frames[0]->payload);
        self::assertSame(ProtocolFrameType::Pong, $frames[1]->type);
    }

    // ─── Max Frame Size Limit ───────────────────────────────────────────

    /**
     * Verifies parser rejects MSG frames that exceed the configured max frame size.
     */
    public function testRejectsMsgFrameExceedingMaxSize(): void
    {
        $parser = new ProtocolParser(maxFrameSize: 10);

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('MSG frame payload size is invalid');

        $parser->push("MSG subject 1 20\r\n" . str_repeat('x', 20) . "\r\n");
    }

    /**
     * Verifies parser rejects HMSG frames that exceed the configured max frame size.
     */
    public function testRejectsHmsgFrameExceedingMaxSize(): void
    {
        $parser = new ProtocolParser(maxFrameSize: 10);

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('HMSG frame payload size is invalid');

        $parser->push("HMSG subject 1 5 20\r\n" . str_repeat('x', 20) . "\r\n");
    }

    /**
     * Verifies parser uses default 8 MiB max frame size.
     */
    public function testDefaultMaxFrameSizeAllowsLargePayloads(): void
    {
        $parser = new ProtocolParser();
        $payload = str_repeat('x', 1024);

        $frames = $parser->push("MSG subject 1 1024\r\n{$payload}\r\n");

        self::assertCount(1, $frames);
        self::assertNotNull($frames[0]->payload);
        self::assertSame(1024, strlen($frames[0]->payload));
    }

    public function testRejectsNonNumericMsgSid(): void
    {
        $parser = new ProtocolParser();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Invalid sid');

        // A non-numeric sid must be rejected, not coerced to 0 and routed to a phantom subscription.
        $parser->push("MSG subject xyz 5\r\nhello\r\n");
    }

    public function testRejectsNonNumericMsgSize(): void
    {
        $parser = new ProtocolParser();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Invalid payload size');

        $parser->push("MSG subject 1 abc\r\nhello\r\n");
    }

    public function testRejectsNegativeMsgSize(): void
    {
        $parser = new ProtocolParser();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Invalid payload size');

        $parser->push("MSG subject 1 -5\r\nhello\r\n");
    }

    public function testRejectsHmsgHeaderBytesExceedingTotal(): void
    {
        $parser = new ProtocolParser();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('header bytes exceed total bytes');

        $parser->push("HMSG subject 1 20 10\r\n1234567890\r\n");
    }

    public function testBuffersSubCapControlLineWithoutCrlf(): void
    {
        $parser = new ProtocolParser();

        // A partial control line with no CRLF yet is buffered (waiting for more), not rejected.
        $frames = $parser->push(str_repeat('A', 1000));

        self::assertSame([], $frames);
    }

    public function testRejectsUnterminatedControlLineExceedingBound(): void
    {
        $parser = new ProtocolParser();

        // A peer streaming >1 MiB without a CRLF must be rejected, not buffered unbounded (OOM guard).
        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Control line exceeds maximum length');

        $parser->push(str_repeat('A', 1048576 + 1));
    }

    public function testResyncsPastMalformedControlLineInsteadOfPoisoning(): void
    {
        $parser = new ProtocolParser();

        try {
            $parser->push("BADOP foo\r\n");
            self::fail('Expected ProtocolException for an unsupported control frame');
        } catch (ProtocolException) {
            // Expected: the offending line is rejected.
        }

        // The bad line must have been consumed (resynced), so a subsequent valid frame parses
        // normally instead of re-throwing forever on the poisoned buffer.
        $frames = $parser->push("PING\r\n");

        self::assertCount(1, $frames);
        self::assertSame(ProtocolFrameType::Ping, $frames[0]->type);
    }
}
