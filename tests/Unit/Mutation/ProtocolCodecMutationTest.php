<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use Composer\InstalledVersions;
use IDCT\NATS\Auth\NkeySeedSigner;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Protocol\ProtocolCodec;
use IDCT\NATS\Tests\Support\FixedNonceSigner;

/**
 * Mutation-killing tests for {@see ProtocolCodec}. Each method pins the exact observable
 * behaviour that an Infection mutant would change, so that the test passes on the real code
 * but fails on the mutated code.
 */
final class ProtocolCodecMutationTest extends \PHPUnit\Framework\TestCase
{
    /**
     * The CONNECT payload must advertise the static client metadata fields verbatim.
     */
    public function testConnectAdvertisesLangAndProtocolAndHeaders(): void
    {
        $codec = new ProtocolCodec();

        // verbose/pedantic are set to true so the ArrayItem mutants (which drop the key and
        // replace it with a positional "0"/"1" comparison result) are unambiguously observable.
        $result = $codec->encodeConnect(new NatsOptions(verbose: true, pedantic: true));

        // kills ArrayItemRemoval @ 37 (drops the 'lang' => 'php' entry)
        self::assertStringContainsString('"lang":"php"', $result);

        // kills DecrementInteger/IncrementInteger @ 40 (protocol 1 -> 0 / 2)
        self::assertStringContainsString('"protocol":1', $result);
        self::assertStringNotContainsString('"protocol":0', $result);
        self::assertStringNotContainsString('"protocol":2', $result);

        // kills ArrayItem @ 41 ('verbose' => $v becomes the comparison 'verbose' > $v, dropping the key)
        self::assertStringContainsString('"verbose":true', $result);

        // kills ArrayItem @ 42 ('pedantic' => $v becomes 'pedantic' > $v, dropping the key)
        self::assertStringContainsString('"pedantic":true', $result);

        // kills TrueValue @ 43 ('headers' => true -> false)
        self::assertStringContainsString('"headers":true', $result);
        self::assertStringNotContainsString('"headers":false', $result);
    }

    /**
     * In the JWT branch the nkey field is emitted only when nkey is a non-empty string. With a null
     * nkey the field must be absent; the `&&` -> `||` mutant would emit `"nkey":null`.
     */
    public function testJwtBranchOmitsNkeyWhenNkeyIsNull(): void
    {
        $codec = new ProtocolCodec();
        $options = new NatsOptions(
            jwt: 'jwt-value',
            nonceSigner: new FixedNonceSigner('sig:'),
            nkey: null,
        );

        $result = $codec->encodeConnect($options, 'nonce-1');

        // Sanity: we really are on the JWT path.
        self::assertStringContainsString('"jwt":"jwt-value"', $result);

        // kills LogicalAnd @ 83 (nkey !== null && nkey !== '' -> || would add "nkey":null)
        self::assertStringNotContainsString('"nkey"', $result);
    }

    /**
     * The seed-signer/nkey mismatch guard must NOT fire when no explicit nkey is configured, even
     * if a NkeySeedSigner is present. The `&&` -> `||` mutant on the outer guard makes the first
     * sub-expression always true, so it would throw a spurious mismatch error here.
     */
    public function testSeedSignerMismatchGuardDoesNotFireWithoutExplicitNkey(): void
    {
        if (!function_exists('sodium_crypto_sign_seed_keypair')) {
            self::markTestSkipped('sodium extension is required for NkeySeedSigner tests.');
        }

        // Seed whose derived public key is non-null; with nkey=null the L104 guard's first
        // sub-expression (nkey !== null && nkey !== '') is false, so the guard must stay quiet.
        $signer = new NkeySeedSigner('SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY');

        $codec = new ProtocolCodec();
        $options = new NatsOptions(nkey: null, nonceSigner: $signer);

        // kills LogicalAnd @ 104 ((nkey !== null && nkey !== '') -> (nkey !== null || nkey !== '')
        // which is always true and would throw the mismatch ProtocolException here).
        $result = $codec->encodeConnect($options, 'server-nonce');

        self::assertStringStartsWith('CONNECT ', $result);
        self::assertStringNotContainsString('"nkey"', $result);
    }

    /**
     * When Composer's InstalledVersions metadata is available (as it is in this test run), the
     * resolved client version must be the package's installed pretty-version, NOT the in-source
     * FALLBACK_CLIENT_VERSION constant ('2.4.0').
     *
     * This single behaviour distinguishes the real code from every clientVersion() mutant that
     * forces the fallback path: the IfNegation @ 121, the NotIdentical/LogicalAnd*-Negation
     * variants @ 124, and the ReturnRemoval @ 125 all return '2.4.0' instead of the installed
     * version.
     */
    public function testConnectAdvertisesInstalledVersionNotFallback(): void
    {
        self::assertTrue(
            class_exists(InstalledVersions::class),
            'These mutants are only killable when Composer runtime metadata is present.',
        );
        $installed = InstalledVersions::getPrettyVersion('idct/php-nats-jetstream-client');
        self::assertNotNull($installed);
        self::assertNotSame('', $installed);
        // Guard against an accidental coincidence where the installed version equals the fallback.
        self::assertNotSame('2.4.0', $installed, 'Installed version must differ from the fallback for this test to discriminate.');

        $result = (new ProtocolCodec())->encodeConnect(new NatsOptions());

        // kills IfNegation @ 121, NotIdentical @ 124 (=== null), NotIdentical @ 124 (=== ''),
        // LogicalAndAllSubExprNegation @ 124, LogicalAndNegation @ 124, ReturnRemoval @ 125.
        self::assertStringContainsString('"version":' . json_encode($installed), $result);
        self::assertStringNotContainsString('"version":"2.4.0"', $result);
    }

    /**
     * HPUB advertises headerBytes and the total (headers + payload) byte count. The `+` -> `-`
     * mutant turns the total into headerBytes - payloadLen, corrupting the frame's size header.
     */
    public function testHeaderPublishTotalBytesIsHeaderPlusPayload(): void
    {
        $codec = new ProtocolCodec();

        // headerBlock is 17 bytes, payload "abcd" is 4 bytes => total must be 21 (not 13).
        $headerBlock = "NATS/1.0\r\nK:v\r\n\r\n";
        self::assertSame(17, strlen($headerBlock));

        $frame = $codec->encodeHeaderPublishBlock('subj', 'abcd', $headerBlock);

        // kills Plus @ 210 (headerBytes + strlen(payload) -> headerBytes - strlen(payload))
        self::assertSame("HPUB subj 17 21\r\nNATS/1.0\r\nK:v\r\n\r\nabcd\r\n", $frame);
    }

    /**
     * parseServerInfo decodes the INFO JSON with a maximum nesting depth of exactly 512.
     * A payload nested 511 levels deep parses fine at depth 512 but FAILS at depth 511, so the
     * DecrementInteger mutant (512 -> 511) would make this throw instead of returning a ServerInfo.
     */
    public function testParseServerInfoAcceptsNestingAtDepthBoundary(): void
    {
        $codec = new ProtocolCodec();
        $payload = self::nestedJson(511);

        // kills DecrementInteger @ 263 (depth 512 -> 511 would throw \JsonException here)
        $info = $codec->parseServerInfo('INFO ' . $payload);

        self::assertInstanceOf(\IDCT\NATS\Protocol\ServerInfo::class, $info);
    }

    /**
     * Conversely, a payload nested 512 levels deep exceeds the depth-512 limit and must throw.
     * The IncrementInteger mutant (512 -> 513) would raise the limit just enough to accept it,
     * so the absence of the expected \JsonException distinguishes the mutant.
     */
    public function testParseServerInfoRejectsNestingBeyondDepthBoundary(): void
    {
        $codec = new ProtocolCodec();
        $payload = self::nestedJson(512);

        // kills IncrementInteger @ 263 (depth 512 -> 513 would accept this instead of throwing)
        $this->expectException(\JsonException::class);

        $codec->parseServerInfo('INFO ' . $payload);
    }

    /**
     * Builds a JSON string nested {@code $levels} objects deep: {"a":{"a":...1...}}.
     */
    private static function nestedJson(int $levels): string
    {
        $json = str_repeat('{"a":', $levels) . '1' . str_repeat('}', $levels);

        return $json;
    }
}
