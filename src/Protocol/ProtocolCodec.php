<?php

declare(strict_types=1);

namespace IDCT\NATS\Protocol;

use Composer\InstalledVersions;
use IDCT\NATS\Auth\NkeySeedSigner;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsHeaders;
use IDCT\NATS\Exception\ProtocolException;

/**
 * Encodes client-side NATS protocol commands into wire-format frames.
 *
 * This codec is responsible for CONNECT negotiation payloads and command line
 * serialization used by publish/subscribe and request/reply flows.
 */
final class ProtocolCodec
{
    /**
     * Reported in CONNECT when the installed package version cannot be resolved at runtime (e.g. a
     * from-source checkout). Bump to the current major.minor as part of the release checklist so a
     * source build does not advertise a stale version in the server's connz/monitoring.
     */
    private const FALLBACK_CLIENT_VERSION = '2.4.0';

    /**
     * Builds the CONNECT frame payload for the initial client handshake.
     *
     * @param array{user?:string,pass?:string,token?:string} $urlCredentials Credentials parsed from the
     *        server URL's userinfo (#37). They take precedence over the static {@see NatsOptions} user/
     *        pass/token, but dynamic providers still win for token/jwt.
     */
    public function encodeConnect(NatsOptions $options, ?string $serverNonce = null, array $urlCredentials = []): string
    {
        $payload = [
            'lang' => 'php',
            'version' => self::clientVersion(),
            'protocol' => 1,
            'verbose' => $options->verbose,
            'pedantic' => $options->pedantic,
            'headers' => true,
            'no_responders' => true,
            'echo' => !$options->noEcho,
            'name' => $options->name,
        ];

        // Credential precedence: a dynamic provider (re-invoked each (re)connect) wins, then a
        // credential parsed from the server URL (#37), then the static option.
        $token = $options->tokenProvider !== null
            ? ($options->tokenProvider)()
            : ($urlCredentials['token'] ?? $options->token);
        $jwt = $options->jwtProvider !== null ? ($options->jwtProvider)() : $options->jwt;
        $user = $urlCredentials['user'] ?? $options->username;
        $pass = $urlCredentials['pass'] ?? $options->password;

        if ($token !== null) {
            $payload['auth_token'] = $token;
        }

        if ($user !== null) {
            $payload['user'] = $user;
        }

        if ($pass !== null) {
            $payload['pass'] = $pass;
        }

        if ($jwt !== null) {
            $payload['jwt'] = $jwt;

            if ($options->nonceSigner === null) {
                throw new ProtocolException('JWT authentication requires a nonce signer');
            }

            if ($serverNonce === null || $serverNonce === '') {
                throw new ProtocolException('JWT authentication requires server nonce from INFO');
            }

            $payload['sig'] = $options->nonceSigner->sign($serverNonce);

            if ($options->nkey !== null && $options->nkey !== '') {
                $payload['nkey'] = $options->nkey;
            }
        } elseif ($options->nkey !== null && $options->nkey !== '') {
            // Standalone NKey authentication (Ed25519 challenge-response without JWT).
            $payload['nkey'] = $options->nkey;

            if ($options->nonceSigner === null) {
                throw new ProtocolException('NKey authentication requires a nonce signer');
            }

            if ($serverNonce === null || $serverNonce === '') {
                throw new ProtocolException('NKey authentication requires server nonce from INFO');
            }

            $payload['sig'] = $options->nonceSigner->sign($serverNonce);
        }

        // Catch a copy/paste mismatch locally: if a seed signer is configured alongside an explicit
        // nkey, the nkey must equal the public key the seed derives - otherwise the server would reject
        // the handshake with an opaque auth error.
        if ($options->nkey !== null && $options->nkey !== ''
            && $options->nonceSigner instanceof NkeySeedSigner
            && $options->nonceSigner->publicKey() !== $options->nkey
        ) {
            throw new ProtocolException('Configured nkey does not match the public key derived from the NKey seed');
        }

        return sprintf("CONNECT %s\r\n", json_encode($payload, JSON_THROW_ON_ERROR));
    }

    /**
     * Resolves the client library version to advertise in CONNECT (shown in server `connz`/
     * monitoring), preferring the installed Composer package version and falling back to a constant
     * when package metadata is unavailable (e.g. running from source).
     */
    private static function clientVersion(): string
    {
        if (class_exists(InstalledVersions::class)) {
            try {
                $version = InstalledVersions::getPrettyVersion('idct/php-nats-jetstream-client');
                if ($version !== null && $version !== '') {
                    return $version;
                }
            } catch (\OutOfBoundsException) {
                // Package not registered with Composer's runtime metadata; fall back below.
            }
        }

        return self::FALLBACK_CLIENT_VERSION;
    }

    /**
     * Encodes a protocol PING command.
     */
    public function encodePing(): string
    {
        return "PING\r\n";
    }

    /**
     * Encodes a protocol PONG command.
     */
    public function encodePong(): string
    {
        return "PONG\r\n";
    }

    /**
     * Encodes a SUB command for plain or queue subscriptions.
     */
    public function encodeSubscribe(string $subject, int $sid, ?string $queue = null): string
    {
        return ($queue === null)
            ? sprintf("SUB %s %d\r\n", $subject, $sid)
            : sprintf("SUB %s %s %d\r\n", $subject, $queue, $sid);
    }

    /**
     * Encodes an UNSUB command with optional max delivery limit.
     */
    public function encodeUnsubscribe(int $sid, ?int $maxMessages = null): string
    {
        return ($maxMessages === null)
            ? sprintf("UNSUB %d\r\n", $sid)
            : sprintf("UNSUB %d %d\r\n", $sid, $maxMessages);
    }

    /**
     * Encodes a PUB frame with optional reply subject.
     */
    public function encodePublish(string $subject, string $payload, ?string $replyTo = null): string
    {
        $size = strlen($payload);

        if ($replyTo === null) {
            return sprintf("PUB %s %d\r\n%s\r\n", $subject, $size, $payload);
        }

        return sprintf("PUB %s %s %d\r\n%s\r\n", $subject, $replyTo, $size, $payload);
    }

    /**
     * Encodes an HPUB frame with headers and optional reply subject.
     *
     * @param array<string,string> $headers
     */
    public function encodeHeaderPublish(
        string $subject,
        string $payload,
        array $headers,
        ?string $replyTo = null,
    ): string {
        return $this->encodeHeaderPublishBlock($subject, $payload, NatsHeaders::toWireBlock($headers), $replyTo);
    }

    /**
     * Builds an HPUB frame from an ALREADY-encoded header wire block, so callers that also need the
     * block's size (or that retry) build and validate it once instead of re-running toWireBlock().
     */
    public function encodeHeaderPublishBlock(
        string $subject,
        string $payload,
        string $headerBlock,
        ?string $replyTo = null,
    ): string {
        $headerBytes = strlen($headerBlock);
        $totalBytes = $headerBytes + strlen($payload);

        if ($replyTo === null) {
            return sprintf(
                "HPUB %s %d %d\r\n%s%s\r\n",
                $subject,
                $headerBytes,
                $totalBytes,
                $headerBlock,
                $payload,
            );
        }

        return sprintf(
            "HPUB %s %s %d %d\r\n%s%s\r\n",
            $subject,
            $replyTo,
            $headerBytes,
            $totalBytes,
            $headerBlock,
            $payload,
        );
    }

    /**
     * Extracts and validates the payload section from an INFO line.
     *
     * @return array{command:string,payload:string}
     */
    public function decodeInfoLine(string $line): array
    {
        $line = trim($line);

        if (!str_starts_with($line, 'INFO ')) {
            throw new ProtocolException('Expected INFO line from server');
        }

        $payload = substr($line, 5);

        if ($payload === '') {
            throw new ProtocolException('INFO payload cannot be empty');
        }

        return ['command' => 'INFO', 'payload' => $payload];
    }

    /**
     * Parses server INFO JSON into a typed capabilities object.
     */
    public function parseServerInfo(string $line): ServerInfo
    {
        $decoded = $this->decodeInfoLine($line);
        /** @var array<string,mixed> $data */
        $data = json_decode($decoded['payload'], true, 512, JSON_THROW_ON_ERROR);

        return ServerInfo::fromInfoPayload($data);
    }
}
