<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

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
}
