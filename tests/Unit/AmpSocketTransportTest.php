<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Socket\ConnectContext;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Transport\AmpSocketTransport;
use PHPUnit\Framework\TestCase;

final class AmpSocketTransportTest extends TestCase
{
    /**
     * Verifies write/read/close are safe no-ops when no socket has been connected.
     */
    public function testWriteReadCloseWithoutSocket(): void
    {
        $transport = new AmpSocketTransport();

        $transport->write('PING\r\n')->await();
        self::assertSame('', $transport->readLine()->await());
        $transport->close()->await();

        // Ensure idempotent close also remains safe.
        $transport->close()->await();
        self::assertSame('', $transport->readLine()->await());
    }

    /**
     * Verifies non-TLS DSNs keep connect context unchanged.
     */
    public function testWithTlsContextReturnsOriginalContextWhenTlsNotRequired(): void
    {
        $transport = new AmpSocketTransport(new NatsOptions(tlsRequired: false));
        $context = new ConnectContext();

        $returned = $this->invokeWithTlsContext($transport, $context, 'nats://127.0.0.1:4222');

        self::assertSame($context, $returned);
    }

    /**
     * Verifies TLS is enabled when using tls:// scheme.
     */
    public function testWithTlsContextBuildsTlsContextFromTlsScheme(): void
    {
        $transport = new AmpSocketTransport(new NatsOptions());
        $context = new ConnectContext();

        $returned = $this->invokeWithTlsContext($transport, $context, 'tls://example.org:4443');

        self::assertNotSame($context, $returned);
        self::assertInstanceOf(ConnectContext::class, $returned);
    }

    /**
     * Verifies explicit TLS options path executes (peer override, no peer verify, CA, client cert).
     */
    public function testWithTlsContextUsesExplicitTlsOptions(): void
    {
        $ca = tempnam(sys_get_temp_dir(), 'ca_');
        $cert = tempnam(sys_get_temp_dir(), 'cert_');
        $key = tempnam(sys_get_temp_dir(), 'key_');
        self::assertNotFalse($ca);
        self::assertNotFalse($cert);
        self::assertNotFalse($key);

        file_put_contents($ca, 'dummy-ca');
        file_put_contents($cert, 'dummy-cert');
        file_put_contents($key, 'dummy-key');

        try {
            $options = new NatsOptions(
                tlsRequired: true,
                tlsVerifyPeer: false,
                tlsPeerName: 'override.example.org',
                tlsCaFile: $ca,
                tlsCertFile: $cert,
                tlsKeyFile: $key,
                tlsKeyPassphrase: 'secret',
            );

            $transport = new AmpSocketTransport($options);
            $context = new ConnectContext();

            $returned = $this->invokeWithTlsContext($transport, $context, 'nats://127.0.0.1:4222');

            self::assertNotSame($context, $returned);
            self::assertInstanceOf(ConnectContext::class, $returned);
        } finally {
            @unlink($ca);
            @unlink($cert);
            @unlink($key);
        }
    }

    /**
     * Verifies connect propagates errors for invalid DSN inputs.
     */
    public function testConnectThrowsOnInvalidDsn(): void
    {
        $transport = new AmpSocketTransport();

        $this->expectException(\Throwable::class);
        $transport->connect('invalid-dsn', 50)->await();
    }

    /**
     * Verifies tls:// URIs are rewritten to tcp:// for Amp while preserving TLS semantics.
     */
    public function testNormalizeSocketUriRewritesTlsScheme(): void
    {
        $transport = new AmpSocketTransport();

        self::assertSame('tcp://example.org:4443', $this->invokeNormalizeSocketUri($transport, 'tls://example.org:4443'));
        self::assertSame('tcp://example.org:4222', $this->invokeNormalizeSocketUri($transport, 'tcp://example.org:4222'));
    }

    private function invokeWithTlsContext(AmpSocketTransport $transport, ConnectContext $context, string $dsn): ConnectContext
    {
        $method = new \ReflectionMethod(AmpSocketTransport::class, 'withTlsContext');

        /** @var ConnectContext $result */
        $result = $method->invoke($transport, $context, $dsn);

        return $result;
    }

    private function invokeNormalizeSocketUri(AmpSocketTransport $transport, string $dsn): string
    {
        $method = new \ReflectionMethod(AmpSocketTransport::class, 'normalizeSocketUri');

        /** @var string $result */
        $result = $method->invoke($transport, $dsn);

        return $result;
    }
}
