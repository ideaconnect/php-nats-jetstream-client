<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Auth\NkeySeedSigner;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Protocol\ProtocolCodec;
use IDCT\NATS\Tests\Support\FixedNonceSigner;
use PHPUnit\Framework\TestCase;

final class ProtocolCodecTest extends TestCase
{
    /**
     * Verifies CONNECT encoding contains the configured client name field.
     */
    public function testEncodeConnectContainsName(): void
    {
        $codec = new ProtocolCodec();
        $options = new NatsOptions(name: 'test-client');

        $result = $codec->encodeConnect($options);

        self::assertStringStartsWith('CONNECT ', $result);
        self::assertStringContainsString('"name":"test-client"', $result);
    }

    public function testEncodeHeaderPublishBlockMatchesEncodeHeaderPublish(): void
    {
        $codec = new ProtocolCodec();
        $headers = ['KV-Operation' => 'PUT', 'Nats-Rollup' => 'sub'];
        $block = \IDCT\NATS\Core\NatsHeaders::toWireBlock($headers);

        // The precomputed-block variant must produce byte-identical frames (with and without replyTo).
        self::assertSame(
            $codec->encodeHeaderPublish('subj', 'body', $headers),
            $codec->encodeHeaderPublishBlock('subj', 'body', $block),
        );
        self::assertSame(
            $codec->encodeHeaderPublish('subj', 'body', $headers, '_INBOX.r'),
            $codec->encodeHeaderPublishBlock('subj', 'body', $block, '_INBOX.r'),
        );
    }

    public function testEncodeConnectAdvertisesResolvedClientVersion(): void
    {
        $result = (new ProtocolCodec())->encodeConnect(new NatsOptions());

        // The CONNECT version is sourced from the installed package, not the old hardcoded literal.
        self::assertMatchesRegularExpression('/"version":"[^"]+"/', $result);
        self::assertStringNotContainsString('0.1.0-dev', $result);
    }

    /**
     * Verifies CONNECT encoding includes username/password fields.
     */
    public function testEncodeConnectContainsPasswordAuthFields(): void
    {
        $codec = new ProtocolCodec();
        $options = new NatsOptions(username: 'alice', password: 's3cr3t');

        $result = $codec->encodeConnect($options);

        self::assertStringContainsString('"user":"alice"', $result);
        self::assertStringContainsString('"pass":"s3cr3t"', $result);
    }

    /**
     * Verifies CONNECT encoding includes token authentication field.
     */
    public function testEncodeConnectContainsTokenAuthField(): void
    {
        $codec = new ProtocolCodec();
        $options = new NatsOptions(token: 'token-123');

        $result = $codec->encodeConnect($options);

        self::assertStringContainsString('"auth_token":"token-123"', $result);
    }

    /**
     * Verifies a tokenProvider is resolved per-encode and overrides the static token (#24).
     */
    public function testEncodeConnectUsesTokenProviderPerConnect(): void
    {
        $codec = new ProtocolCodec();
        $calls = 0;
        $options = new NatsOptions(
            token: 'static-token',
            tokenProvider: static function () use (&$calls): string {
                $calls++;

                return 'rotated-token-' . $calls;
            },
        );

        $first = $codec->encodeConnect($options);
        $second = $codec->encodeConnect($options);

        self::assertStringContainsString('"auth_token":"rotated-token-1"', $first);
        self::assertStringContainsString('"auth_token":"rotated-token-2"', $second);
        self::assertStringNotContainsString('static-token', $first);
        self::assertSame(2, $calls);
    }

    /**
     * Verifies URL-embedded user/password credentials are used and override the static options (#37).
     */
    public function testEncodeConnectUsesUrlUserPassword(): void
    {
        $codec = new ProtocolCodec();
        $options = new NatsOptions(username: 'static-user', password: 'static-pass');

        $result = $codec->encodeConnect($options, null, ['user' => 'url-user', 'pass' => 'url-pass']);

        self::assertStringContainsString('"user":"url-user"', $result);
        self::assertStringContainsString('"pass":"url-pass"', $result);
        self::assertStringNotContainsString('static-user', $result);
    }

    /**
     * Verifies a URL-embedded token is used as auth_token (#37).
     */
    public function testEncodeConnectUsesUrlToken(): void
    {
        $codec = new ProtocolCodec();

        $result = $codec->encodeConnect(new NatsOptions(), null, ['token' => 'url-token']);

        self::assertStringContainsString('"auth_token":"url-token"', $result);
    }

    /**
     * Verifies a jwtProvider is resolved per-encode and overrides the static JWT (#24).
     */
    public function testEncodeConnectUsesJwtProviderPerConnect(): void
    {
        $codec = new ProtocolCodec();
        $calls = 0;
        $options = new NatsOptions(
            jwt: 'static-jwt',
            nonceSigner: new FixedNonceSigner('sig:'),
            jwtProvider: static function () use (&$calls): string {
                $calls++;

                return 'fresh-jwt-' . $calls;
            },
        );

        $result = $codec->encodeConnect($options, 'nonce-1');

        self::assertStringContainsString('"jwt":"fresh-jwt-1"', $result);
        self::assertStringContainsString('"sig":"sig:nonce-1"', $result);
        self::assertStringNotContainsString('static-jwt', $result);
    }

    /**
     * Verifies CONNECT encoding includes JWT auth fields signed with server nonce.
     */
    public function testEncodeConnectContainsJwtAuthFields(): void
    {
        $codec = new ProtocolCodec();
        $options = new NatsOptions(
            jwt: 'jwt-token-value',
            nkey: 'UABC123',
            nonceSigner: new FixedNonceSigner('sig:'),
        );

        $result = $codec->encodeConnect($options, 'nonce-1');

        self::assertStringContainsString('"jwt":"jwt-token-value"', $result);
        self::assertStringContainsString('"nkey":"UABC123"', $result);
        self::assertStringContainsString('"sig":"sig:nonce-1"', $result);
    }

    /**
     * Verifies JWT auth requires a nonce signer and server nonce.
     */
    public function testEncodeConnectJwtRequiresSignerAndNonce(): void
    {
        $codec = new ProtocolCodec();

        $this->expectException(ProtocolException::class);
        $codec->encodeConnect(new NatsOptions(jwt: 'jwt-token-value'), 'nonce-1');
    }

    /**
     * Verifies PUB encoding uses payload length and CRLF framing.
     */
    public function testEncodePublishWithoutReply(): void
    {
        $codec = new ProtocolCodec();

        $result = $codec->encodePublish('orders.created', 'abc');

        self::assertSame("PUB orders.created 3\r\nabc\r\n", $result);
    }

    /**
     * Verifies INFO payload parsing maps expected server fields.
     */
    public function testParseServerInfo(): void
    {
        $codec = new ProtocolCodec();
        $line = 'INFO {"server_id":"X","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}';

        $info = $codec->parseServerInfo($line);

        self::assertSame('X', $info->serverId);
        self::assertSame('n1', $info->serverName);
        self::assertTrue($info->jetStreamEnabled);
        self::assertSame(1048576, $info->maxPayload);
    }

    /**
     * Verifies HPUB encoding includes header and total byte counts.
     */
    public function testEncodeHeaderPublish(): void
    {
        $codec = new ProtocolCodec();

        $result = $codec->encodeHeaderPublish(
            subject: 'orders.created',
            payload: 'abc',
            headers: ['Nats-Schedule' => '@at 2030-01-01T00:00:00Z'],
        );

        self::assertStringStartsWith('HPUB orders.created ', $result);
        self::assertStringContainsString("NATS/1.0\r\nNats-Schedule:@at 2030-01-01T00:00:00Z\r\n\r\nabc\r\n", $result);
    }

    /**
     * Verifies CONNECT encoding includes echo=false when noEcho is enabled.
     */
    public function testEncodeConnectContainsNoEchoFalse(): void
    {
        $codec = new ProtocolCodec();
        $options = new NatsOptions(noEcho: true);

        $result = $codec->encodeConnect($options);

        self::assertStringContainsString('"echo":false', $result);
    }

    /**
     * Verifies CONNECT encoding includes echo=true when noEcho is disabled (default).
     */
    public function testEncodeConnectDefaultEchoTrue(): void
    {
        $codec = new ProtocolCodec();
        $options = new NatsOptions();

        $result = $codec->encodeConnect($options);

        self::assertStringContainsString('"echo":true', $result);
    }

    /**
     * Verifies standalone NKey auth includes nkey and sig fields without JWT.
     */
    public function testEncodeConnectStandaloneNkeyAuth(): void
    {
        $codec = new ProtocolCodec();
        $options = new NatsOptions(
            nkey: 'UABC123PUBLIC',
            nonceSigner: new FixedNonceSigner('signed:'),
        );

        $result = $codec->encodeConnect($options, 'server-nonce-1');

        self::assertStringContainsString('"nkey":"UABC123PUBLIC"', $result);
        self::assertStringContainsString('"sig":"signed:server-nonce-1"', $result);
        self::assertStringNotContainsString('"jwt"', $result);
    }

    /**
     * Verifies standalone NKey auth requires a nonce signer.
     */
    public function testEncodeConnectNkeyRequiresSigner(): void
    {
        $codec = new ProtocolCodec();
        $options = new NatsOptions(nkey: 'UABC123PUBLIC');

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('NKey authentication requires a nonce signer');

        $codec->encodeConnect($options, 'nonce-1');
    }

    /**
     * Verifies standalone NKey auth requires server nonce.
     */
    public function testEncodeConnectNkeyRequiresServerNonce(): void
    {
        $codec = new ProtocolCodec();
        $options = new NatsOptions(
            nkey: 'UABC123PUBLIC',
            nonceSigner: new FixedNonceSigner('signed:'),
        );

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('NKey authentication requires server nonce from INFO');

        $codec->encodeConnect($options);
    }

    /**
     * Verifies JWT auth with a valid signer but a null nonce throws (line 74).
     *
     * The existing testEncodeConnectJwtRequiresSignerAndNonce covers the missing-signer
     * path (line 70). This test covers the complementary branch: signer is present but
     * the server nonce is null.
     */
    public function testEncodeConnectJwtWithSignerButNullNonceThrows(): void
    {
        $codec = new ProtocolCodec();
        $options = new NatsOptions(
            jwt: 'jwt-value',
            nonceSigner: new FixedNonceSigner('sig:'),
        );

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('JWT authentication requires server nonce from INFO');

        $codec->encodeConnect($options, null);
    }

    /**
     * Verifies JWT auth with a valid signer but an empty-string nonce throws (line 74).
     */
    public function testEncodeConnectJwtWithSignerButEmptyNonceThrows(): void
    {
        $codec = new ProtocolCodec();
        $options = new NatsOptions(
            jwt: 'jwt-value',
            nonceSigner: new FixedNonceSigner('sig:'),
        );

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('JWT authentication requires server nonce from INFO');

        $codec->encodeConnect($options, '');
    }

    /**
     * Verifies that configuring an NKey that does not match the public key derived from the
     * NkeySeedSigner seed throws (line 104).
     *
     * The mismatch guard fires regardless of whether JWT is also set; here we use the JWT
     * path so both jwt and nkey are present in the payload and the seed-signer check runs.
     */
    public function testEncodeConnectNkeyMismatchWithSeedSignerThrows(): void
    {
        if (!function_exists('sodium_crypto_sign_seed_keypair')) {
            self::markTestSkipped('sodium extension is required for NkeySeedSigner tests.');
        }

        // Known-good seed whose public key is UDXU4RCSJNZOIQHZNWXHXORDPRTGNJAHAHFRGZNEEJCPQTT2M7NLCNF4.
        $signer = new NkeySeedSigner('SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY');

        $codec = new ProtocolCodec();
        $options = new NatsOptions(
            jwt: 'jwt-value',
            // Intentionally wrong public key - does not match the seed above.
            nkey: 'UABC123WRONGKEY',
            nonceSigner: $signer,
        );

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Configured nkey does not match the public key derived from the NKey seed');

        $codec->encodeConnect($options, 'server-nonce');
    }

    /**
     * Verifies that decodeInfoLine throws when the line does not start with 'INFO ' (line 240).
     */
    public function testDecodeInfoLineThrowsOnNonInfoPrefix(): void
    {
        $codec = new ProtocolCodec();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Expected INFO line from server');

        $codec->decodeInfoLine('PING\r\n');
    }

    /**
     * Verifies that a bare 'INFO' line without a trailing space throws the prefix error (line 240).
     *
     * NOTE: The "INFO payload cannot be empty" guard at line 246 is structurally unreachable:
     * trim() strips trailing whitespace, so any trimmed line that passes the str_starts_with(..., 'INFO ')
     * check must contain at least one non-whitespace character after position 5, making substr(..., 5)
     * always non-empty. This test documents the reachable boundary instead.
     */
    public function testDecodeInfoLineThrowsOnBareInfoWithoutSpace(): void
    {
        $codec = new ProtocolCodec();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Expected INFO line from server');

        $codec->decodeInfoLine('INFO');
    }

    /**
     * Verifies the happy path of decodeInfoLine returns the correct command and raw payload string.
     */
    public function testDecodeInfoLineReturnsCommandAndPayload(): void
    {
        $codec = new ProtocolCodec();

        $result = $codec->decodeInfoLine('INFO {"server_id":"X"}');

        self::assertSame('INFO', $result['command']);
        self::assertSame('{"server_id":"X"}', $result['payload']);
    }

    /**
     * Verifies decodeInfoLine trims surrounding whitespace before checking the prefix,
     * so a line ending with CRLF is handled correctly.
     */
    public function testDecodeInfoLineTrimsWhitespace(): void
    {
        $codec = new ProtocolCodec();

        $result = $codec->decodeInfoLine("INFO {\"server_id\":\"Y\"}\r\n");

        self::assertSame('INFO', $result['command']);
        self::assertSame('{"server_id":"Y"}', $result['payload']);
    }
}
