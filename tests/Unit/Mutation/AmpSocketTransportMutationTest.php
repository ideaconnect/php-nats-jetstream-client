<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use Amp\Socket\ClientTlsContext;
use Amp\Socket\ConnectContext;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Transport\AmpSocketTransport;
use IDCT\NATS\Transport\TlsRequiredException;

use function Amp\async;
use function Amp\Socket\listen;

/**
 * Targeted tests that pin behaviors Infection mutated in {@see AmpSocketTransport}.
 *
 * Each method exercises the exact input where the real code and the mutant diverge and asserts the
 * specific observable difference (a stored field, a built TLS/connect context, the bytes a peer
 * receives, or a thrown exception). All async work runs in-process against a local loopback
 * listener — no Docker, no real NATS server, no wall-clock timing.
 */
final class AmpSocketTransportMutationTest extends \PHPUnit\Framework\TestCase
{
    /**
     * The connect timeout floor (max(1, $timeoutMs)) must clamp to exactly 1, not 0 and not 2.
     *
     * The clamped value is stored in the private lastConnectTimeoutMs field, which we read back via
     * reflection after a successful connect to a local listener.
     */
    public function testConnectTimeoutFloorClampsToOne(): void
    {
        // kills IncrementInteger @ line 51: timeoutMs=1 => real max(1,1)=1, mutant max(2,1)=2.
        $field = $this->connectAndReadTimeoutField(1);
        self::assertSame(1, $field);

        // kills DecrementInteger @ line 51: timeoutMs=0 => real max(1,0)=1 (connect succeeds);
        // mutant max(0,0)=0 => withConnectTimeout(0/1000) throws and connect() never returns 0.
        $fieldZero = $this->connectAndReadTimeoutField(0);
        self::assertSame(1, $fieldZero);
    }

    /**
     * Handshake-first must NOT run the TLS handshake when no TLS context was configured.
     *
     * The guard is `tlsContextConfigured && tlsHandshakeFirst`. With a plaintext connection and
     * handshakeFirst=true the operands differ (false && true), so the mutant `||` (false || true)
     * would call establishTls() on a plaintext socket and throw TlsRequiredException at connect time.
     */
    public function testHandshakeFirstWithoutTlsContextDoesNotUpgrade(): void
    {
        [$transport, $server] = $this->connectPlaintext(
            new NatsOptions(tlsRequired: false, tlsHandshakeFirst: true),
        );

        try {
            // kills LogicalAnd @ line 64: real => no establishTls (no throw, TLS inactive);
            // mutant (||) => establishTls() on plaintext socket => TlsRequiredException.
            self::assertFalse($transport->tlsActive());
        } finally {
            $transport->close()->await();
            $server->close();
        }
    }

    /**
     * The TlsRequiredException message must be assembled from its three operands in order, intact.
     *
     * Asserting the exact full string kills every concat reorder and every operand-removal mutant on
     * the message: a reorder changes the order, a removal drops a chunk.
     */
    public function testTlsRequiredExceptionMessageIsExact(): void
    {
        [$transport, $server] = $this->connectPlaintext(new NatsOptions());

        $expected = 'TLS upgrade requested but no TLS context was configured at connect time; '
            . 'set NatsOptions tlsRequired (or use a tls:// server and provide CA/cert materials) '
            . 'when the server requires TLS';

        $message = null;
        try {
            $transport->upgradeTls()->await();
        } catch (TlsRequiredException $e) {
            $message = $e->getMessage();
        } finally {
            $transport->close()->await();
            $server->close();
        }

        // kills Concat (x2) and ConcatOperandRemoval (x2) @ line 104.
        self::assertSame($expected, $message);
    }

    /**
     * write() must actually push bytes to the socket; the peer must receive the exact payload.
     */
    public function testWriteSendsBytesToPeer(): void
    {
        $server = listen('tcp://127.0.0.1:0');
        $address = (string) $server->getAddress();

        $received = null;
        $accepted = async(static function () use ($server, &$received): void {
            $client = $server->accept();
            if ($client !== null) {
                $received = $client->read();
            }
        });

        $transport = new AmpSocketTransport(new NatsOptions());
        $transport->connect('tcp://' . $address, 1000)->await();

        try {
            $transport->write("PING\r\n")->await();
            $accepted->await();

            // kills MethodCallRemoval @ line 120: without ->write() the peer receives nothing.
            self::assertSame("PING\r\n", $received);
        } finally {
            $transport->close()->await();
            $server->close();
        }
    }

    /**
     * An uppercase TLS scheme must be lowercased before the scheme comparison, so TLS:// still
     * enables TLS. Without strtolower(), 'TLS' !== 'tls' and the connect context is returned
     * unchanged (no TLS context attached).
     */
    public function testUppercaseTlsSchemeStillEnablesTls(): void
    {
        $transport = new AmpSocketTransport(new NatsOptions(tlsRequired: false));
        $context = new ConnectContext();

        $returned = $this->invokeWithTlsContext($transport, $context, 'TLS://example.org:4443');

        // kills UnwrapStrToLower @ line 168: real lowercases => TLS attached; mutant keeps 'TLS' =>
        // requiresTls false => original context returned with no TLS context.
        self::assertNotSame($context, $returned);
        self::assertNotNull($returned->getTlsContext());
    }

    /**
     * A non-empty tlsPeerName override must be used verbatim and must NOT fall back to the DSN host.
     *
     * The guard `peerName === null || peerName === ''` is false for a non-empty override, so the
     * fallback is skipped. Both OR mutants would (wrongly) take the fallback and use the DSN host.
     */
    public function testNonEmptyPeerNameOverrideIsKept(): void
    {
        $transport = new AmpSocketTransport(new NatsOptions(
            tlsRequired: true,
            tlsPeerName: 'override.example.org',
        ));

        $returned = $this->invokeWithTlsContext($transport, new ConnectContext(), 'nats://realhost.example.org:4222');
        $tls = $returned->getTlsContext();
        self::assertInstanceOf(ClientTlsContext::class, $tls);

        // kills Identical and LogicalOrAllSubExprNegation @ line 176: both mutants would replace the
        // override with the DSN host 'realhost.example.org'.
        self::assertSame('override.example.org', $tls->getPeerName());
    }

    /**
     * When no peer name override is set, the peer name must fall back to the DSN host.
     *
     * The Coalesce mutant `'' ?? parse_url(...)` collapses to '' (empty peer name); the real code
     * uses the DSN host.
     */
    public function testEmptyPeerNameFallsBackToDsnHost(): void
    {
        $transport = new AmpSocketTransport(new NatsOptions(
            tlsRequired: true,
            tlsPeerName: null,
        ));

        $returned = $this->invokeWithTlsContext($transport, new ConnectContext(), 'nats://realhost.example.org:4222');
        $tls = $returned->getTlsContext();
        self::assertInstanceOf(ClientTlsContext::class, $tls);

        // kills Coalesce @ line 177: mutant yields '' instead of the DSN host.
        self::assertSame('realhost.example.org', $tls->getPeerName());
    }

    /**
     * Peer verification must stay ON by default (tlsVerifyPeer=true) — withoutPeerVerification()
     * must NOT be called. The LogicalNot mutant drops the negation, disabling verification.
     */
    public function testPeerVerificationStaysEnabledByDefault(): void
    {
        $transport = new AmpSocketTransport(new NatsOptions(
            tlsRequired: true,
            tlsVerifyPeer: true,
        ));

        $returned = $this->invokeWithTlsContext($transport, new ConnectContext(), 'tls://h.example.org:4443');
        $tls = $returned->getTlsContext();
        self::assertInstanceOf(ClientTlsContext::class, $tls);

        // kills LogicalNot @ line 182: mutant calls withoutPeerVerification() when verify=true.
        self::assertTrue($tls->hasPeerVerification());
    }

    /**
     * A configured, non-empty CA file must be applied to the TLS context.
     *
     * The guard is `tlsCaFile !== null && tlsCaFile !== ''`. A non-empty path makes it true, so the
     * CA is set. Mutants that flip an operand to ===, negate the whole expression, or AND-all-negate
     * make the guard false and skip the CA.
     */
    public function testNonEmptyCaFileIsApplied(): void
    {
        $transport = new AmpSocketTransport(new NatsOptions(
            tlsRequired: true,
            tlsCaFile: '/nonexistent/ca.pem',
        ));

        $returned = $this->invokeWithTlsContext($transport, new ConnectContext(), 'tls://h.example.org:4443');
        $tls = $returned->getTlsContext();
        self::assertInstanceOf(ClientTlsContext::class, $tls);

        // kills NotIdentical(left), NotIdentical(right), LogicalAndAllSubExprNegation,
        // LogicalAndNegation @ line 186 — all of which would skip withCaFile().
        self::assertSame('/nonexistent/ca.pem', $tls->getCaFile());
    }

    /**
     * An empty CA file must NOT be applied (the && guard is false for '').
     *
     * The LogicalAnd mutant turns && into ||, which becomes true for '' (since '' !== null) and
     * wrongly calls withCaFile('') — making getCaFile() return '' instead of null.
     */
    public function testEmptyCaFileIsNotApplied(): void
    {
        $transport = new AmpSocketTransport(new NatsOptions(
            tlsRequired: true,
            tlsCaFile: '',
        ));

        $returned = $this->invokeWithTlsContext($transport, new ConnectContext(), 'tls://h.example.org:4443');
        $tls = $returned->getTlsContext();
        self::assertInstanceOf(ClientTlsContext::class, $tls);

        // kills LogicalAnd @ line 186: mutant (||) calls withCaFile('') => getCaFile() === ''.
        self::assertNull($tls->getCaFile());
    }

    /**
     * A configured, non-empty client certificate file must produce a Certificate on the TLS context.
     *
     * The guard is `tlsCertFile !== null && tlsCertFile !== ''`. The NotIdentical(right) mutant
     * (=== '') and the all-subexpr-negation mutant both make the guard false and skip the cert.
     */
    public function testNonEmptyCertFileIsApplied(): void
    {
        $transport = new AmpSocketTransport(new NatsOptions(
            tlsRequired: true,
            tlsCertFile: '/nonexistent/cert.pem',
            tlsKeyFile: '/nonexistent/key.pem',
        ));

        $returned = $this->invokeWithTlsContext($transport, new ConnectContext(), 'tls://h.example.org:4443');
        $tls = $returned->getTlsContext();
        self::assertInstanceOf(ClientTlsContext::class, $tls);

        $certificate = $tls->getCertificate();
        // kills NotIdentical(right) and LogicalAndAllSubExprNegation @ line 190 — both skip the cert.
        self::assertNotNull($certificate);
        self::assertSame('/nonexistent/cert.pem', $certificate->getCertFile());
    }

    /**
     * Connects to a throwaway local listener with the given dial timeout and returns the value the
     * transport stored in its private lastConnectTimeoutMs field.
     */
    private function connectAndReadTimeoutField(int $timeoutMs): int
    {
        $server = listen('tcp://127.0.0.1:0');
        $address = (string) $server->getAddress();

        $accepted = async(static function () use ($server): void {
            $client = $server->accept();
            if ($client !== null) {
                // Keep the accepted connection alive briefly so the client dial completes cleanly.
                $client->read(new TimeoutCancellation(0.2));
            }
        });

        $transport = new AmpSocketTransport(new NatsOptions());
        try {
            $transport->connect('tcp://' . $address, $timeoutMs)->await();

            $property = new \ReflectionProperty(AmpSocketTransport::class, 'lastConnectTimeoutMs');

            /** @var int $value */
            $value = $property->getValue($transport);

            return $value;
        } finally {
            $transport->close()->await();
            try {
                $accepted->await();
            } catch (\Throwable) {
                // The bounded server read may be cancelled; irrelevant to the assertion.
            }
            $server->close();
        }
    }

    /**
     * Opens a local listener, accepts and holds the peer open, and connects a plaintext transport.
     *
     * @return array{0: AmpSocketTransport, 1: \Amp\Socket\ResourceServerSocket}
     */
    private function connectPlaintext(NatsOptions $options): array
    {
        $server = listen('tcp://127.0.0.1:0');
        $address = (string) $server->getAddress();

        async(static function () use ($server): void {
            $client = $server->accept();
            if ($client !== null) {
                // Hold the connection so the client socket stays connected during the test.
                $client->read(new TimeoutCancellation(0.5));
            }
        });

        $transport = new AmpSocketTransport($options);
        $transport->connect('tcp://' . $address, 1000)->await();

        return [$transport, $server];
    }

    private function invokeWithTlsContext(AmpSocketTransport $transport, ConnectContext $context, string $dsn): ConnectContext
    {
        $method = new \ReflectionMethod(AmpSocketTransport::class, 'withTlsContext');

        /** @var ConnectContext $result */
        $result = $method->invoke($transport, $context, $dsn);

        return $result;
    }
}
