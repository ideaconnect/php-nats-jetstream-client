<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Protocol\Enum\ProtocolFrameType;
use IDCT\NATS\Protocol\ProtocolParser;
use PHPUnit\Framework\TestCase;

/**
 * Robustness / fuzz coverage for the protocol parser - the kind of timing/byte-boundary and
 * malformed-input behavior a static review cannot prove. Seeds are fixed so the run is deterministic.
 *
 * Invariants asserted:
 *  - arbitrary/garbage bytes only ever raise {@see ProtocolException} (never a TypeError, division error,
 *    etc.) and never hang (a non-advancing loop would time the suite out);
 *  - a valid multi-frame stream fed one byte at a time reassembles identically to a single push
 *    (byte-boundary independence);
 *  - an unterminated control line is bounded (throws) rather than growing the buffer to OOM;
 *  - an oversized declared MSG payload is rejected;
 *  - the parser resyncs past a malformed control line so a later valid frame still parses (no poison).
 */
final class ProtocolParserFuzzTest extends TestCase
{
    public function testArbitraryBytesOnlyRaiseProtocolException(): void
    {
        mt_srand(20260613);

        $iterations = 3000;
        $completed = 0;
        for ($i = 0; $i < $iterations; $i++) {
            $parser = new ProtocolParser();
            $length = mt_rand(0, 80);
            $chunk = '';
            for ($j = 0; $j < $length; $j++) {
                $chunk .= chr(mt_rand(0, 255));
            }

            try {
                $parser->push($chunk);
            } catch (ProtocolException) {
                // Acceptable: malformed input is reported via the typed protocol exception.
            } catch (\Throwable $e) {
                self::fail(sprintf(
                    'Unexpected %s for input 0x%s: %s',
                    $e::class,
                    bin2hex($chunk),
                    $e->getMessage(),
                ));
            }

            $completed++;
        }

        // Reaching here with every iteration counted proves no non-ProtocolException escaped and the
        // parser never hung (a non-advancing loop would have timed the suite out).
        self::assertSame($iterations, $completed);
    }

    public function testRandomProtocolTokenSoupOnlyRaisesProtocolException(): void
    {
        // Bias the input toward real protocol tokens so the MSG/HMSG/control-line branches are exercised
        // with adversarial spacing, counts, and truncation - not just uniformly random bytes.
        $tokens = [
            'MSG ', 'HMSG ', 'PING', 'PONG', '+OK', '-ERR boom', 'INFO {"a":1}',
            "\r\n", ' ', '0', '5', '999999999', 'subj', 'sid', '_INBOX.x', "\x00", "\xff", 'NATS/1.0',
        ];
        $tokenCount = count($tokens);

        mt_srand(424242);

        $iterations = 3000;
        $completed = 0;
        for ($i = 0; $i < $iterations; $i++) {
            $parser = new ProtocolParser();
            $parts = mt_rand(1, 10);
            $chunk = '';
            for ($p = 0; $p < $parts; $p++) {
                $chunk .= $tokens[mt_rand(0, $tokenCount - 1)];
            }

            try {
                $parser->push($chunk);
                // Feed an empty push too: completes any pending frame without new bytes.
                $parser->push('');
            } catch (ProtocolException) {
                // Acceptable.
            } catch (\Throwable $e) {
                self::fail(sprintf(
                    'Unexpected %s for input %s: %s',
                    $e::class,
                    var_export($chunk, true),
                    $e->getMessage(),
                ));
            }

            $completed++;
        }

        self::assertSame($iterations, $completed);
    }

    public function testByteAtATimeReassemblyMatchesSinglePush(): void
    {
        $headerBlock = "NATS/1.0\r\nX-Trace: abc\r\n\r\n";
        $stream =
            "PING\r\n"
            . "MSG foo 1 5\r\nhello\r\n"
            . "PONG\r\n"
            . sprintf("HMSG bar 2 _INBOX.r %d %d\r\n%sworld\r\n", strlen($headerBlock), strlen($headerBlock) + 5, $headerBlock)
            . "+OK\r\n"
            . "INFO {\"server_id\":\"S\"}\r\n"
            . "MSG baz 3 0\r\n\r\n";

        $expected = (new ProtocolParser())->push($stream);
        self::assertNotEmpty($expected);

        $drip = new ProtocolParser();
        $got = [];
        for ($i = 0, $n = strlen($stream); $i < $n; $i++) {
            foreach ($drip->push($stream[$i]) as $frame) {
                $got[] = $frame;
            }
        }

        self::assertCount(count($expected), $got);
        foreach ($expected as $k => $frame) {
            self::assertSame($frame->type, $got[$k]->type, "frame {$k} type");
            self::assertSame($frame->subject, $got[$k]->subject, "frame {$k} subject");
            self::assertSame($frame->sid, $got[$k]->sid, "frame {$k} sid");
            self::assertSame($frame->replyTo, $got[$k]->replyTo, "frame {$k} replyTo");
            self::assertSame($frame->payload, $got[$k]->payload, "frame {$k} payload");
            self::assertSame($frame->headerBytes, $got[$k]->headerBytes, "frame {$k} headerBytes");
            self::assertSame($frame->totalBytes, $got[$k]->totalBytes, "frame {$k} totalBytes");
            self::assertSame($frame->infoPayload, $got[$k]->infoPayload, "frame {$k} infoPayload");
        }
    }

    public function testUnterminatedControlLineIsBoundedNotUnbounded(): void
    {
        $parser = new ProtocolParser();

        $this->expectException(ProtocolException::class);
        // > 1 MiB of bytes with no CRLF: must throw rather than buffer unboundedly.
        $parser->push(str_repeat('A', 1024 * 1024 + 16));
    }

    public function testOversizedDeclaredMsgPayloadIsRejected(): void
    {
        $parser = new ProtocolParser(64);

        $this->expectException(ProtocolException::class);
        // Declares a 1000-byte payload against a 64-byte bound; rejected at header-parse time.
        $parser->push("MSG subject 1 1000\r\n");
    }

    public function testParserResyncsAfterMalformedControlLine(): void
    {
        $parser = new ProtocolParser();

        try {
            $parser->push("GARBAGE-NOT-A-VERB\r\n");
            self::fail('Expected a ProtocolException for the unsupported control line.');
        } catch (ProtocolException) {
            // Expected: the offending line is consumed (resync), not left to re-throw forever.
        }

        // A subsequent valid frame must parse, proving the buffer resynced past the bad line.
        $frames = $parser->push("PING\r\n");
        self::assertCount(1, $frames);
        self::assertSame(ProtocolFrameType::Ping, $frames[0]->type);
    }
}
