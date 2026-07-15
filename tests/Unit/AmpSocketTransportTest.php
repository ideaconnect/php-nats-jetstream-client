<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\CancelledException;
use Amp\Socket\ConnectContext;
use Amp\TimeoutCancellation;
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

    /**
     * The configured readChunkSizeBytes controls the MAXIMUM a single socket read may return (#119): a
     * 128 KiB chunk size lets one read pull far more than Amp's 8 KiB default off the wire, dividing the
     * per-chunk syscall/fiber/parser overhead, while an 8 KiB chunk size caps every read at 8 KiB.
     *
     * FALSIFIABILITY: without the setChunkSize() call the transport keeps Amp's 8 KiB default, so no
     * read could ever exceed 8 KiB and the 128 KiB assertion (a single read > 8 KiB) would fail.
     */
    public function testReadChunkSizeControlsMaxBytesPerRead(): void
    {
        $payload = str_repeat('x', 256 * 1024); // 256 KiB, far larger than one 8 KiB chunk

        $measure = static function (int $chunkBytes) use ($payload): array {
            $server = listen('tcp://127.0.0.1:0');
            $address = (string) $server->getAddress();

            async(static function () use ($server, $payload): void {
                $client = $server->accept();
                if ($client !== null) {
                    $client->write($payload);
                    // Hold the peer open so the reader drains the whole payload before EOF.
                    delay(0.3);
                    $client->close();
                }
            });

            $transport = new AmpSocketTransport(new NatsOptions(readChunkSizeBytes: $chunkBytes));
            $transport->connect('tcp://' . $address, 1000)->await();

            $received = 0;
            $reads = 0;
            $maxRead = 0;
            try {
                while ($received < strlen($payload)) {
                    $chunk = $transport->readLine(new TimeoutCancellation(2.0))->await();
                    $length = strlen($chunk);
                    if ($length === 0) {
                        break;
                    }
                    $received += $length;
                    $reads++;
                    $maxRead = max($maxRead, $length);
                }
            } catch (\Throwable) {
                // A late EOF/timeout after the payload is drained is irrelevant to the measurement.
            } finally {
                $transport->close()->await();
                $server->close();
            }

            return ['reads' => $reads, 'maxRead' => $maxRead, 'received' => $received];
        };

        $large = $measure(128 * 1024);
        $small = $measure(8 * 1024);

        self::assertSame(strlen($payload), $large['received'], 'the full payload must arrive with a 128 KiB chunk size');
        self::assertSame(strlen($payload), $small['received'], 'the full payload must arrive with an 8 KiB chunk size');

        // A 128 KiB chunk size lets a single read exceed 8 KiB; an 8 KiB chunk size never does.
        self::assertGreaterThan(8 * 1024, $large['maxRead'], '128 KiB chunk size must allow a single read larger than 8 KiB');
        self::assertLessThanOrEqual(8 * 1024, $small['maxRead'], '8 KiB chunk size must cap every read at 8 KiB');

        // Fewer, larger reads for the larger chunk size (256 KiB needs >= 32 reads at 8 KiB).
        self::assertLessThan($small['reads'], $large['reads'], 'a larger chunk size must divide the read count');
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
     * A bounded read whose cancellation fires (no bytes arrive within the window) must surface the
     * timeout as a CancelledException through the returned future - NOT as EOF/TransportClosedException
     * and NOT as an empty string. The connection layer relies on this to tell a bounded read
     * (handshake slice, heartbeat wait) apart from a genuine peer close. Guards the #163 inline
     * readLine() (no async() wrapper): the passed Cancellation must still reach the underlying read
     * and its exception must still flow back through await() unchanged.
     */
    public function testReadLineSurfacesReadTimeoutAsCancelledExceptionNotEof(): void
    {
        $server = listen('tcp://127.0.0.1:0');
        $address = (string) $server->getAddress();

        // Accept and hold the peer open WITHOUT sending any bytes: the only way readLine() can return
        // is via the cancellation - the socket is live (not '') and the peer never closes (not EOF).
        $accepted = async(static function () use ($server): void {
            $client = $server->accept();
            if ($client !== null) {
                delay(0.5);
                $client->close();
            }
        });

        $transport = new AmpSocketTransport(new NatsOptions());
        $transport->connect('tcp://' . $address, 1000)->await();

        try {
            $transport->readLine(new TimeoutCancellation(0.05))->await();
            self::fail('readLine() must not return normally when its read is cancelled by timeout');
        } catch (\Throwable $e) {
            // A read timeout MUST surface as CancelledException, never as EOF
            // (TransportClosedException) - the connection layer distinguishes the two.
            self::assertInstanceOf(
                CancelledException::class,
                $e,
                'readLine() must surface a read timeout as CancelledException, not EOF',
            );
        } finally {
            $transport->close()->await();
            try {
                $accepted->await();
            } catch (\Throwable) {
                // The held-open peer may be torn down mid-delay; irrelevant to the assertion.
            }
            $server->close();
        }
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
