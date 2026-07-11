<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Socket\ConnectContext;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Transport\AmpSocketTransport;
use IDCT\NATS\Transport\TlsRequiredException;
use IDCT\NATS\Transport\TransportClosedException;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\delay;
use function Amp\Socket\listen;

final class AmpSocketTransportTest extends TestCase
{
    /**
     * Writing without a socket must throw: a silent no-op would confirm publishes/ACKs that never
     * reached any socket (#124). Reads stay '' (handshake/idle polling) and close stays idempotent.
     */
    public function testWriteWithoutSocketThrowsWhileReadAndCloseStaySafe(): void
    {
        $transport = new AmpSocketTransport();

        try {
            $transport->write("PING\r\n")->await();
            self::fail('expected TransportClosedException for a write without a socket');
        } catch (TransportClosedException $e) {
            self::assertSame('Transport is not connected', $e->getMessage());
        }

        self::assertSame('', $transport->readLine()->await());
        $transport->close()->await();

        // Ensure idempotent close also remains safe, and a post-close write throws the same way.
        $transport->close()->await();
        self::assertSame('', $transport->readLine()->await());
        $this->expectException(TransportClosedException::class);
        $transport->write("PING\r\n")->await();
    }

    /**
     * Verifies upgradeTls() short-circuits when TLS was never configured (no socket to upgrade),
     * so the standard non-TLS connect flow can call it unconditionally.
     */
    public function testUpgradeTlsIsNoOpWhenTlsNotConfigured(): void
    {
        $transport = new AmpSocketTransport(new NatsOptions());

        // No connect(): there is no socket and no TLS context, so the handshake must not run.
        $transport->upgradeTls()->await();

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
        // nats:// is accepted directly so the transport is usable standalone.
        self::assertSame('tcp://example.org:4222', $this->invokeNormalizeSocketUri($transport, 'nats://example.org:4222'));
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

    /**
     * Verifies a real peer close (EOF) surfaces as TransportClosedException rather than being
     * collapsed into '' - the root-cause regression test for read-path reconnect.
     */
    public function testReadLineThrowsTransportClosedOnPeerEof(): void
    {
        $server = listen('tcp://127.0.0.1:0');
        $address = (string) $server->getAddress();

        async(static function () use ($server): void {
            $client = $server->accept();
            if ($client !== null) {
                $client->write("hello\r\n");
                $client->close();
            }
        });

        $transport = new AmpSocketTransport(new NatsOptions());
        $transport->connect('tcp://' . $address, 1000)->await();

        $collected = '';
        $threw = false;

        try {
            // Read the bytes, then the next read must observe EOF and throw.
            for ($i = 0; $i < 50; $i++) {
                $collected .= $transport->readLine()->await();
            }
        } catch (TransportClosedException) {
            $threw = true;
        } finally {
            $transport->close()->await();
            $server->close();
        }

        self::assertStringContainsString('hello', $collected);
        self::assertTrue($threw, 'readLine() must throw TransportClosedException on peer EOF');
    }

    /**
     * Verifies upgradeTls() on a connected plaintext socket with no TLS materials fails fast with
     * TlsRequiredException, rather than leaving the socket plaintext (which would leak a CONNECT).
     */
    public function testUpgradeTlsThrowsWhenConnectedWithoutTlsContext(): void
    {
        $server = listen('tcp://127.0.0.1:0');
        $address = (string) $server->getAddress();

        // Accept and hold the connection open so the client socket stays connected during upgrade.
        async(static function () use ($server): void {
            $client = $server->accept();
            if ($client !== null) {
                delay(0.5);
                $client->close();
            }
        });

        $transport = new AmpSocketTransport(new NatsOptions());
        $transport->connect('tcp://' . $address, 1000)->await();

        $threw = false;
        try {
            $transport->upgradeTls()->await();
        } catch (TlsRequiredException $e) {
            $threw = true;
            self::assertStringContainsString('TLS upgrade requested but no TLS context', $e->getMessage());
        } finally {
            $transport->close()->await();
            $server->close();
        }

        self::assertTrue($threw, 'upgradeTls() without TLS materials must throw TlsRequiredException');
    }
}
