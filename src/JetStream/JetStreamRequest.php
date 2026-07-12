<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream;

use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\NatsException;

/**
 * Shared no-responders normalization for JetStream request/reply paths. A request to a subject that
 * is not bound to any stream (or a server with JetStream disabled) answers with the no-responders
 * sentinel, which the connection surfaces as a bare {@see NatsException}. Every JetStream path must
 * translate that into a {@see JetStreamException} with code 503 so callers catching JetStreamException
 * see one exception taxonomy, regardless of which request site produced it (#161).
 *
 * {@see JetStreamContext::jsRequest()} and the header-request sites in KeyValueBucket /
 * ObjectStoreBucket / BatchPublisher all funnel their no-responders case through here.
 */
final class JetStreamRequest
{
    /**
     * Performs a header request/reply and normalizes a no-responders failure to JetStreamException(503).
     * Returns the reply message unchanged on success (callers apply their own reply-shape checks).
     *
     * @param array<string,string> $headers
     */
    public static function withHeaders(NatsClient $client, string $subject, string $payload, array $headers): NatsMessage
    {
        try {
            return $client->requestWithHeaders($subject, $payload, $headers)->await();
        } catch (JetStreamException $e) {
            throw $e;
        } catch (NatsException $e) {
            throw self::normalizeNoResponders($e, $subject);
        }
    }

    /**
     * Maps a no-responders NatsException to JetStreamException(503); returns any other exception
     * unchanged so the caller can rethrow it as-is.
     */
    public static function normalizeNoResponders(NatsException $e, string $subject): NatsException
    {
        if (str_contains($e->getMessage(), 'No responders')) {
            return new JetStreamException(
                'No JetStream responder for subject ' . $subject
                . ' (the subject is not bound to a stream, or JetStream is not enabled)',
                503,
                $e,
            );
        }

        return $e;
    }
}
