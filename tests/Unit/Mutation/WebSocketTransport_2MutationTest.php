<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use Amp\Socket\ClientTlsContext;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Transport\WebSocketTransport;
use PHPUnit\Framework\TestCase;

/**
 * Mutation-killing tests for src/Transport/WebSocketTransport.php (chunk 2).
 *
 * Targets the pure HTTP-upgrade-request builder and the private TLS-context builder,
 * driven directly (the latter via reflection) without any socket.
 */
final class WebSocketTransport_2MutationTest extends TestCase
{
    /**
     * Invokes the private buildTlsContext($host) on a freshly constructed transport.
     */
    private function buildTlsContext(NatsOptions $options, string $host = 'host.example'): ClientTlsContext
    {
        $transport = new WebSocketTransport($options);
        $method = new \ReflectionMethod(WebSocketTransport::class, 'buildTlsContext');

        $result = $method->invoke($transport, $host);
        self::assertInstanceOf(ClientTlsContext::class, $result);

        return $result;
    }

    /**
     * The $compression parameter defaults to false: omitting it must NOT emit the
     * permessage-deflate extension offer.
     *
     * // kills FalseValue @ line 346  (default false -> true would always offer compression)
     */
    public function testCompressionDefaultsToOffWhenArgumentOmitted(): void
    {
        $request = WebSocketTransport::buildUpgradeRequest('h', 80, '/', 'k');

        self::assertStringNotContainsString('permessage-deflate', $request);
        self::assertStringNotContainsString('Sec-WebSocket-Extensions', $request);
    }

    /**
     * Sanity counterpart: explicitly passing true still emits the offer (guards against an
     * over-broad fix that would break the real feature).
     */
    public function testCompressionOfferEmittedWhenExplicitlyEnabled(): void
    {
        $request = WebSocketTransport::buildUpgradeRequest('h', 80, '/', 'k', [], true);

        self::assertStringContainsString('Sec-WebSocket-Extensions: permessage-deflate', $request);
    }

    /**
     * A non-empty, non-null tlsPeerName must be honored verbatim (NOT overwritten by host).
     *
     * Real: ($peerName === null || $peerName === '') is false -> keep 'custom.peer'.
     * Identical mutant ($peerName !== '') -> true -> overwrite with host.
     * LogicalOrAllSubExprNegation (!(=null) || !(='')) -> true -> overwrite with host.
     *
     * // kills Identical @ line 387
     * // kills LogicalOrAllSubExprNegation @ line 387
     */
    public function testNonEmptyTlsPeerNameIsHonoredOverHost(): void
    {
        $ctx = $this->buildTlsContext(
            new NatsOptions(tlsPeerName: 'custom.peer'),
            'host.example',
        );

        self::assertSame('custom.peer', $ctx->getPeerName());
    }

    /**
     * An empty-string tlsPeerName must fall back to the host (the '' === '' branch).
     *
     * Real: $peerName === '' is true -> use host.
     * This pins the second sub-expression so a mutation that drops it is caught.
     */
    public function testEmptyTlsPeerNameFallsBackToHost(): void
    {
        $ctx = $this->buildTlsContext(
            new NatsOptions(tlsPeerName: ''),
            'host.example',
        );

        self::assertSame('host.example', $ctx->getPeerName());
    }

    /**
     * A null tlsPeerName falls back to the host (the null branch).
     */
    public function testNullTlsPeerNameFallsBackToHost(): void
    {
        $ctx = $this->buildTlsContext(
            new NatsOptions(tlsPeerName: null),
            'host.example',
        );

        self::assertSame('host.example', $ctx->getPeerName());
    }

    /**
     * tlsVerifyPeer=false must disable peer verification; tlsVerifyPeer=true must keep it on.
     *
     * Real: if (!$tlsVerifyPeer) withoutPeerVerification().
     * LogicalNot mutant: if ($tlsVerifyPeer) -> inverts both outcomes.
     *
     * // kills LogicalNot @ line 393
     */
    public function testPeerVerificationFollowsOption(): void
    {
        $disabled = $this->buildTlsContext(new NatsOptions(tlsVerifyPeer: false));
        self::assertFalse($disabled->hasPeerVerification());

        $enabled = $this->buildTlsContext(new NatsOptions(tlsVerifyPeer: true));
        self::assertTrue($enabled->hasPeerVerification());
    }

    /**
     * A non-empty tlsCaFile must be applied to the context.
     *
     * Real: (caFile !== null && caFile !== '') true -> withCaFile(path).
     * NotIdentical (=== null): first sub-expr false -> skip -> getCaFile() null.
     * NotIdentical (=== ''): second sub-expr false -> skip -> getCaFile() null.
     * LogicalAndAllSubExprNegation ((===null) && (===''))): false -> skip -> null.
     * LogicalAndNegation (!(cond)): false -> skip -> null.
     *
     * // kills NotIdentical (=== null) @ line 397
     * // kills NotIdentical (=== '') @ line 397
     * // kills LogicalAndAllSubExprNegation @ line 397
     * // kills LogicalAndNegation @ line 397
     */
    public function testNonEmptyCaFileIsApplied(): void
    {
        $ctx = $this->buildTlsContext(new NatsOptions(tlsCaFile: '/etc/ssl/ca.pem'));

        self::assertSame('/etc/ssl/ca.pem', $ctx->getCaFile());
    }

    /**
     * An empty-string tlsCaFile must NOT be applied (the && guard rejects '').
     *
     * Real: ('' !== null) && ('' !== '') -> true && false -> false -> getCaFile() stays null.
     * LogicalAnd mutant (|| instead of &&): true || false -> true -> withCaFile('') -> getCaFile()==''.
     *
     * // kills LogicalAnd @ line 397
     */
    public function testEmptyCaFileIsNotApplied(): void
    {
        $ctx = $this->buildTlsContext(new NatsOptions(tlsCaFile: ''));

        self::assertNull($ctx->getCaFile());
    }

    /**
     * A null tlsCaFile leaves the context without a CA file (default state).
     */
    public function testNullCaFileLeavesContextUnchanged(): void
    {
        $ctx = $this->buildTlsContext(new NatsOptions(tlsCaFile: null));

        self::assertNull($ctx->getCaFile());
    }

    /**
     * A non-empty tlsCertFile must install a client Certificate (mTLS).
     *
     * Real: (certFile !== null && certFile !== '') true -> withCertificate(...).
     * NotIdentical (=== ''): second sub-expr false -> skip -> getCertificate() null.
     * LogicalAndAllSubExprNegation: always false -> skip -> getCertificate() null.
     *
     * // kills NotIdentical (=== '') @ line 401
     * // kills LogicalAndAllSubExprNegation @ line 401
     */
    public function testNonEmptyCertFileInstallsCertificate(): void
    {
        $ctx = $this->buildTlsContext(new NatsOptions(
            tlsCertFile: '/etc/ssl/client.crt',
            tlsKeyFile: '/etc/ssl/client.key',
        ));

        $certificate = $ctx->getCertificate();
        self::assertNotNull($certificate);
        self::assertSame('/etc/ssl/client.crt', $certificate->getCertFile());
        self::assertSame('/etc/ssl/client.key', $certificate->getKeyFile());
    }

    /**
     * A null tlsCertFile installs no certificate (default, no mTLS).
     */
    public function testNullCertFileInstallsNoCertificate(): void
    {
        $ctx = $this->buildTlsContext(new NatsOptions(tlsCertFile: null));

        self::assertNull($ctx->getCertificate());
    }
}
