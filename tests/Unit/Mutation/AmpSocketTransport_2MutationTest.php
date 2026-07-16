<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use Amp\Socket\ConnectContext;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Tests\Support\ScriptedChunkSocket;
use IDCT\NATS\Transport\AmpSocketTransport;

/**
 * Targeted tests that pin behaviors Infection mutated in {@see AmpSocketTransport} (chunk 2).
 *
 * Each method drives the exact input where the real code and the mutant diverge and asserts the
 * specific observable difference (the socket actually being closed, a thrown TypeError). No Docker,
 * no real NATS server, no wall-clock timing - an in-memory Socket double or a pure withTlsContext()
 * call is enough.
 */
final class AmpSocketTransport_2MutationTest extends \PHPUnit\Framework\TestCase
{
    /**
     * close() must actually close the underlying socket, not merely drop the reference.
     *
     * We inject an in-memory Socket double, await close(), and assert the socket observed a real
     * close() call. The transport nulls its own reference, so we assert on our retained handle.
     *
     * kills MethodCallRemoval @ line 209: removing `$this->socket?->close()` leaves the socket open
     * (the async closure only runs `$this->socket = null`), so isClosed() stays false.
     */
    public function testCloseClosesUnderlyingSocket(): void
    {
        $socket = new ScriptedChunkSocket([]);
        self::assertFalse($socket->isClosed(), 'precondition: socket starts open');

        $transport = new AmpSocketTransport(new NatsOptions());
        $socketProperty = new \ReflectionProperty(AmpSocketTransport::class, 'socket');
        $socketProperty->setValue($transport, $socket);

        $transport->close()->await();

        // Real code invoked $socket->close(); the MethodCallRemoval mutant never does.
        self::assertTrue($socket->isClosed());
    }

    /**
     * The DSN host must be cast to string before it becomes the TLS peer-name fallback, so a
     * malformed DSN (parse_url() returns bool false for the host) still yields a string ('').
     *
     * With tlsRequired the branch reaches line 229 regardless of scheme parsing. For 'tls://:4443'
     * parse_url(PHP_URL_HOST) is false; `false ?? ''` stays false, so without the (string) cast a
     * bool reaches clientTlsContextFromOptions(string $fallbackPeerName) and, under strict_types,
     * raises a TypeError. The real (string) cast turns false into '', so the call succeeds and the
     * peer name is the empty string.
     *
     * kills CastString @ line 229: dropping (string) passes bool false to a string parameter.
     */
    public function testMalformedDsnHostIsCastToStringForPeerName(): void
    {
        $transport = new AmpSocketTransport(new NatsOptions(tlsRequired: true));

        $method = new \ReflectionMethod(AmpSocketTransport::class, 'withTlsContext');

        /** @var ConnectContext $returned */
        $returned = $method->invoke($transport, new ConnectContext(), 'tls://:4443');

        $tls = $returned->getTlsContext();
        self::assertNotNull($tls, 'tlsRequired must attach a TLS context even for a hostless DSN');

        // Real code casts the false host to '' (a string); the mutant would have thrown a TypeError
        // before returning, so reaching this string assertion at all is the kill.
        self::assertSame('', $tls->getPeerName());
    }
}
