<?php

declare(strict_types=1);

namespace IDCT\NATS\Protocol;

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
     * Builds the CONNECT frame payload for the initial client handshake.
     */
    public function encodeConnect(NatsOptions $options, ?string $serverNonce = null): string
    {
        $payload = [
            'lang' => 'php',
            'version' => '0.1.0-dev',
            'protocol' => 1,
            'verbose' => $options->verbose,
            'pedantic' => $options->pedantic,
            'headers' => true,
            'no_responders' => true,
            'echo' => !$options->noEcho,
            'name' => $options->name,
        ];

        if ($options->token !== null) {
            $payload['auth_token'] = $options->token;
        }

        if ($options->username !== null) {
            $payload['user'] = $options->username;
        }

        if ($options->password !== null) {
            $payload['pass'] = $options->password;
        }

        if ($options->jwt !== null) {
            $payload['jwt'] = $options->jwt;

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

        return sprintf("CONNECT %s\r\n", json_encode($payload, JSON_THROW_ON_ERROR));
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
        $headerBlock = NatsHeaders::toWireBlock($headers);
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
