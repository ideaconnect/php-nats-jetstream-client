<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Protocol\ServerInfo;

/**
 * Mutation-killing tests for {@see ServerInfo}. Each method pins the exact observable
 * behaviour that an Infection mutant would change, so that the test passes on the real
 * code but fails on the mutated code.
 */
final class ServerInfoMutationTest extends \PHPUnit\Framework\TestCase
{
    /**
     * The boolean constructor defaults must all be false when callers omit them.
     * kills FalseValue @ 37 (tlsRequired default false -> true)
     * kills FalseValue @ 38 (tlsAvailable default false -> true)
     * kills FalseValue @ 39 (lameDuckMode default false -> true)
     */
    public function testBooleanConstructorDefaultsAreFalse(): void
    {
        // Only the required positional args are passed; nonce + the three trailing
        // booleans rely on their defaults.
        $info = new ServerInfo(
            serverId: 'sid',
            serverName: 'sname',
            version: '1.2.3',
            jetStreamEnabled: true,
            maxPayload: 99,
            headersSupported: true,
        );

        self::assertFalse($info->tlsRequired);
        self::assertFalse($info->tlsAvailable);
        self::assertFalse($info->lameDuckMode);
    }

    /**
     * connect_urls filtering keeps only non-empty strings: the predicate must be a
     * logical AND of both conditions, not an OR.
     *
     * With `is_string($url) && $url !== ''` (real code):
     *   - 'a'   -> true  AND true  -> kept
     *   - ''    -> true  AND false -> dropped
     *   - 123   -> false AND true  -> dropped
     *
     * With the mutated `is_string($url) || $url !== ''`:
     *   - ''    -> true  OR false  -> kept (regression)
     *   - 123   -> false OR true   -> kept (regression)
     *
     * kills LogicalAnd @ 53
     */
    public function testConnectUrlsKeepsOnlyNonEmptyStrings(): void
    {
        $info = ServerInfo::fromInfoPayload([
            'connect_urls' => ['a.example:4222', '', 123, 'b.example:4222'],
        ]);

        self::assertSame(['a.example:4222', 'b.example:4222'], $info->connectUrls);
    }

    /**
     * jetstream defaults to false when the INFO payload omits the key.
     * kills FalseValue @ 63 (jetstream ?? false -> ?? true)
     */
    public function testJetStreamDefaultsToFalseWhenMissing(): void
    {
        $info = ServerInfo::fromInfoPayload([]);

        self::assertFalse($info->jetStreamEnabled);
    }

    /**
     * max_payload defaults to exactly 0 when the INFO payload omits the key.
     * kills DecrementInteger @ 64 (?? 0 -> ?? -1)
     * kills IncrementInteger @ 64 (?? 0 -> ?? 1)
     */
    public function testMaxPayloadDefaultsToZeroWhenMissing(): void
    {
        $info = ServerInfo::fromInfoPayload([]);

        self::assertSame(0, $info->maxPayload);
    }

    /**
     * headers defaults to false when missing, AND a present truthy value is honoured.
     * The missing case pins the FalseValue mutant; the present-true case pins the
     * Coalesce mutant which short-circuits to a constant false regardless of payload.
     *
     * kills FalseValue @ 65 (headers ?? false -> ?? true)  [missing -> stays false]
     * kills Coalesce  @ 65 (headers ?? false -> false ?? headers) [present true -> stays true]
     */
    public function testHeadersSupportedDefaultsFalseAndHonoursPresentTrue(): void
    {
        $missing = ServerInfo::fromInfoPayload([]);
        self::assertFalse($missing->headersSupported);

        $present = ServerInfo::fromInfoPayload(['headers' => true]);
        self::assertTrue($present->headersSupported);
    }

    /**
     * A non-string nonce in the payload must be coerced to string via the (string) cast.
     * Removing the cast would leave the int 123 as an int, breaking the typed property
     * contract; we assert both the exact string value and the string type.
     *
     * kills CastString @ 66 ((string) $payload['nonce'] -> $payload['nonce'])
     */
    public function testNonceIsCastToString(): void
    {
        $info = ServerInfo::fromInfoPayload(['nonce' => 123]);

        self::assertSame('123', $info->nonce);
    }

    /**
     * tls_available defaults to false when missing, AND a present truthy value is honoured.
     *
     * kills FalseValue @ 68 (tls_available ?? false -> ?? true)  [missing -> stays false]
     * kills Coalesce  @ 68 (tls_available ?? false -> false ?? tls_available) [present true -> stays true]
     */
    public function testTlsAvailableDefaultsFalseAndHonoursPresentTrue(): void
    {
        $missing = ServerInfo::fromInfoPayload([]);
        self::assertFalse($missing->tlsAvailable);

        $present = ServerInfo::fromInfoPayload(['tls_available' => true]);
        self::assertTrue($present->tlsAvailable);
    }
}
