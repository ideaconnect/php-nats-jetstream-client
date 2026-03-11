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
