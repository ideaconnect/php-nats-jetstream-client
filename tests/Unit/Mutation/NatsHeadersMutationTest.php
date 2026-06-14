<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Core\NatsHeaders;

/**
 * Targeted tests that kill surviving Infection mutants in {@see NatsHeaders}.
 *
 * Each test pins the exact observable behaviour a specific mutation would change: trimmed bytes,
 * regex anchoring (caret/dollar), and the break-vs-continue end-of-block boundary.
 */
final class NatsHeadersMutationTest extends \PHPUnit\Framework\TestCase
{
    /**
     * Encoded value whitespace must be trimmed before the colon-joined line.
     */
    public function testToWireBlockTrimsSurroundingWhitespaceFromValue(): void
    {
        // kills UnwrapTrim @ line 44: trim($singleValue) -> $singleValue would emit "X-Pad:  v  ".
        $raw = NatsHeaders::toWireBlock(['X-Pad' => '  v  ']);

        self::assertStringContainsString("X-Pad:v\r\n", $raw);
        self::assertStringNotContainsString('X-Pad:  v  ', $raw);
    }

    /**
     * fromWireBlockMulti must only treat a status line anchored at the START of the first line.
     */
    public function testFromWireBlockMultiRequiresStatusLineAnchoredAtStart(): void
    {
        // kills PregMatchRemoveCaret @ line 73: dropping ^ would match "NATS/1.0 503" anywhere in
        // the first line and wrongly synthesise a Status from a non-status first line.
        $raw = "garbage NATS/1.0 503 Down\r\n\r\n";

        $result = NatsHeaders::fromWireBlockMulti($raw);

        self::assertSame([], $result);
        self::assertArrayNotHasKey('Status', $result);
    }

    /**
     * fromWireBlockMulti status regex must be anchored at the END of the first line.
     */
    public function testFromWireBlockMultiRequiresStatusLineAnchoredAtEnd(): void
    {
        // kills PregMatchRemoveDollar @ line 73: a bare LF survives the \r\n split, so the first line
        // is "NATS/1.0 503 Desc\nExtra". With the trailing $ this is NOT a status line; dropping $
        // would match (.*) up to the LF and synthesise Status=503/Description=Desc.
        $raw = "NATS/1.0 503 Desc\nExtra\r\n\r\n";

        $result = NatsHeaders::fromWireBlockMulti($raw);

        self::assertSame([], $result);
        self::assertArrayNotHasKey('Status', $result);
    }

    /**
     * fromWireBlockMulti must trim trailing whitespace from a status description.
     */
    public function testFromWireBlockMultiTrimsStatusDescription(): void
    {
        // kills UnwrapTrim @ line 75: without trim the greedy (.*) keeps the trailing spaces, so
        // Description would be ['No Responders   '] instead of ['No Responders'].
        $result = NatsHeaders::fromWireBlockMulti("NATS/1.0 503 No Responders   \r\n\r\n");

        self::assertSame(['No Responders'], $result['Description'] ?? null);
    }

    /**
     * fromWireBlockMulti must trim whitespace around a header name.
     */
    public function testFromWireBlockMultiTrimsHeaderName(): void
    {
        // kills UnwrapTrim @ line 91: without trim the key would be '  Spaced  ' not 'Spaced'.
        $result = NatsHeaders::fromWireBlockMulti("NATS/1.0\r\n  Spaced  :payload\r\n\r\n");

        self::assertSame(['payload'], $result['Spaced'] ?? null);
        self::assertArrayNotHasKey('  Spaced  ', $result);
    }

    /**
     * fromWireBlockMulti must trim whitespace around a header value.
     */
    public function testFromWireBlockMultiTrimsHeaderValue(): void
    {
        // kills UnwrapTrim @ line 96: without trim the value would be '  mval  ' not 'mval'.
        $result = NatsHeaders::fromWireBlockMulti("NATS/1.0\r\nMName:  mval  \r\n\r\n");

        self::assertSame(['mval'], $result['MName'] ?? null);
    }

    /**
     * fromWireBlock must only treat a status line anchored at the START of the first line.
     */
    public function testFromWireBlockRequiresStatusLineAnchoredAtStart(): void
    {
        // kills PregMatchRemoveCaret @ line 126: dropping ^ would synthesise Status=503 from a first
        // line where "NATS/1.0 503" is not at the start.
        $raw = "garbage NATS/1.0 503 Down\r\n\r\n";

        $result = NatsHeaders::fromWireBlock($raw);

        self::assertSame([], $result);
        self::assertArrayNotHasKey('Status', $result);
    }

    /**
     * fromWireBlock status regex must be anchored at the END of the first line.
     */
    public function testFromWireBlockRequiresStatusLineAnchoredAtEnd(): void
    {
        // kills PregMatchRemoveDollar @ line 126: bare LF in the first line means it is not a pure
        // status line; dropping $ would wrongly match and set Status=503/Description=Desc.
        $raw = "NATS/1.0 503 Desc\nExtra\r\n\r\n";

        $result = NatsHeaders::fromWireBlock($raw);

        self::assertSame([], $result);
        self::assertArrayNotHasKey('Status', $result);
    }

    /**
     * fromWireBlock must trim trailing whitespace from a status description.
     */
    public function testFromWireBlockTrimsStatusDescription(): void
    {
        // kills UnwrapTrim @ line 128: without trim Description would keep the trailing spaces.
        $result = NatsHeaders::fromWireBlock("NATS/1.0 503 No Responders   \r\n\r\n");

        self::assertSame('No Responders', $result['Description'] ?? null);
    }

    /**
     * fromWireBlock must STOP parsing at the first empty line (end of header block), not continue.
     */
    public function testFromWireBlockStopsAtEmptyLineRatherThanContinuing(): void
    {
        // kills Break_ @ line 136: break -> continue would keep parsing body bytes after the blank
        // line separating headers from the payload, so 'After' would leak into the header map.
        $raw = "NATS/1.0\r\nBefore:yes\r\n\r\nAfter:no\r\n";

        $result = NatsHeaders::fromWireBlock($raw);

        self::assertSame('yes', $result['Before'] ?? null);
        self::assertArrayNotHasKey('After', $result);
        self::assertSame(['Before' => 'yes'], $result);
    }

    /**
     * fromWireBlock must trim whitespace around a header name.
     */
    public function testFromWireBlockTrimsHeaderName(): void
    {
        // kills UnwrapTrim @ line 144: without trim the key would be '  Spaced  ' not 'Spaced'.
        $result = NatsHeaders::fromWireBlock("NATS/1.0\r\n  Spaced  :payload\r\n\r\n");

        self::assertSame('payload', $result['Spaced'] ?? null);
        self::assertArrayNotHasKey('  Spaced  ', $result);
    }
}
