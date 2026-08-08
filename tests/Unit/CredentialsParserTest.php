<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Auth\CredentialsParser;
use IDCT\NATS\Exception\NatsException;
use PHPUnit\Framework\TestCase;

final class CredentialsParserTest extends TestCase
{
    public function testParseExtractsJwtAndNkeySeed(): void
    {
        $contents = <<<'CREDS'
            -----BEGIN NATS USER JWT-----
            eyJhbGciOiJlZDI1NTE5LW5rZXkiLCJ0eXAiOiJKV1QifQ.test.signature
            -----END NATS USER JWT-----

            -----BEGIN USER NKEY SEED-----
            SUAM42LQBA2VJFRGZ3LHHK3PPJF3FRC3GPKMRSWO4FEZ3BWBSDX7ZJHPM
            -----END USER NKEY SEED-----
            CREDS;

        $result = CredentialsParser::parse($contents);

        self::assertSame('eyJhbGciOiJlZDI1NTE5LW5rZXkiLCJ0eXAiOiJKV1QifQ.test.signature', $result['jwt']);
        self::assertSame('SUAM42LQBA2VJFRGZ3LHHK3PPJF3FRC3GPKMRSWO4FEZ3BWBSDX7ZJHPM', $result['nkeySeed']);
    }

    public function testParseAcceptsCanonicalNscMarkersWithSixDashEnd(): void
    {
        // Real nsc/.creds output is asymmetric: five dashes on BEGIN, SIX on END. The parser must
        // accept it (the previous five-dash-only regex rejected every genuine credentials file).
        $contents = <<<'CREDS'
            -----BEGIN NATS USER JWT-----
            eyJhbGciOiJlZDI1NTE5LW5rZXkiLCJ0eXAiOiJKV1QifQ.real.jwt
            ------END NATS USER JWT------

            -----BEGIN USER NKEY SEED-----
            SUAM42LQBA2VJFRGZ3LHHK3PPJF3FRC3GPKMRSWO4FEZ3BWBSDX7ZJHPM
            ------END USER NKEY SEED------
            CREDS;

        $result = CredentialsParser::parse($contents);

        self::assertSame('eyJhbGciOiJlZDI1NTE5LW5rZXkiLCJ0eXAiOiJKV1QifQ.real.jwt', $result['jwt']);
        self::assertSame('SUAM42LQBA2VJFRGZ3LHHK3PPJF3FRC3GPKMRSWO4FEZ3BWBSDX7ZJHPM', $result['nkeySeed']);
    }

    public function testFromFileParsesRealNscFixtureWhenPresent(): void
    {
        $fixture = dirname(__DIR__, 2) . '/build/nats/jwt/user.creds';
        if (!is_file($fixture)) {
            self::markTestSkipped('Real .creds fixture not present (run composer fixture:jwt).');
        }

        $result = CredentialsParser::fromFile($fixture);

        self::assertStringStartsWith('ey', $result['jwt']);          // a real JWT
        self::assertStringStartsWith('S', $result['nkeySeed']);      // a NATS NKey seed
    }

    public function testParseRejectsMissingJwtBlock(): void
    {
        $contents = <<<'CREDS'
            -----BEGIN USER NKEY SEED-----
            SUAM42LQBA2VJFRGZ3LHHK3PPJF3FRC3GPKMRSWO4FEZ3BWBSDX7ZJHPM
            -----END USER NKEY SEED-----
            CREDS;

        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('NATS USER JWT block');

        CredentialsParser::parse($contents);
    }

    public function testParseRejectsMissingNkeySeedBlock(): void
    {
        $contents = <<<'CREDS'
            -----BEGIN NATS USER JWT-----
            eyJhbGciOiJlZDI1NTE5LW5rZXkiLCJ0eXAiOiJKV1QifQ.test.signature
            -----END NATS USER JWT-----
            CREDS;

        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('USER NKEY SEED block');

        CredentialsParser::parse($contents);
    }

    public function testFromFileRejectsNonExistentPath(): void
    {
        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('not found or not readable');

        CredentialsParser::fromFile('/nonexistent/path/user.creds');
    }

    /**
     * Pins the defense-in-depth contract of extractBlock(): the block type is matched
     * LITERALLY (preg_quote), never as a regex. A block type containing a regex
     * metacharacter ('A.B') must not match markers whose text merely regex-matches it
     * ('AXB') — neither on the BEGIN marker nor on the END marker.
     *
     * The public block types ('NATS USER JWT', 'USER NKEY SEED') contain no
     * metacharacters, so this contract is only reachable through the private method.
     */
    public function testExtractBlockTreatsBlockTypeLiterallyOnBothMarkers(): void
    {
        $method = new \ReflectionMethod(CredentialsParser::class, 'extractBlock');

        // BEGIN marker regex-matches 'A.B' but is not literally 'A.B': must NOT match.
        $beginMetachar = "-----BEGIN AXB-----\ncontent\n-----END A.B-----";
        self::assertNull($method->invoke(null, $beginMetachar, 'A.B'));

        // END marker regex-matches 'A.B' but is not literally 'A.B': must NOT match.
        $endMetachar = "-----BEGIN A.B-----\ncontent\n-----END AXB-----";
        self::assertNull($method->invoke(null, $endMetachar, 'A.B'));

        // Literal metacharacter markers on both sides DO match.
        $literal = "-----BEGIN A.B-----\ncontent\n-----END A.B-----";
        self::assertSame('content', $method->invoke(null, $literal, 'A.B'));
    }

    public function testFromFileReadsValidCredsFile(): void
    {
        $contents = <<<'CREDS'
            -----BEGIN NATS USER JWT-----
            jwt-token-here
            -----END NATS USER JWT-----

            -----BEGIN USER NKEY SEED-----
            seed-here
            -----END USER NKEY SEED-----
            CREDS;

        $tmpFile = tempnam(sys_get_temp_dir(), 'nats_creds_');
        self::assertNotFalse($tmpFile);
        file_put_contents($tmpFile, $contents);

        try {
            $result = CredentialsParser::fromFile($tmpFile);
            self::assertSame('jwt-token-here', $result['jwt']);
            self::assertSame('seed-here', $result['nkeySeed']);
        } finally {
            unlink($tmpFile);
        }
    }
}
