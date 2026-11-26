<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream;

use IDCT\NATS\Exception\JetStreamException;

/**
 * Constants for JetStream API subject prefixes and helper builders.
 *
 * The constants are the subjects under the default API prefix, `$JS.API.`. A {@see JetStreamContext} made for a
 * JetStream domain or an API prefix sends its requests to the same subjects rebased onto its own prefix
 * ({@see withPrefix()}, {@see JetStreamContext::apiSubject()}).
 */
final class JetStreamApi
{
    /** The prefix every subject constant below starts with: the JetStream API of the connected account, no domain. */
    public const DEFAULT_PREFIX = '$JS.API.';

    public const ACCOUNT_INFO = '$JS.API.INFO';
    public const STREAM_CREATE_PREFIX = '$JS.API.STREAM.CREATE.';
    public const STREAM_INFO_PREFIX = '$JS.API.STREAM.INFO.';
    public const STREAM_UPDATE_PREFIX = '$JS.API.STREAM.UPDATE.';
    public const STREAM_DELETE_PREFIX = '$JS.API.STREAM.DELETE.';
    public const STREAM_MSG_GET_PREFIX = '$JS.API.STREAM.MSG.GET.';
    public const STREAM_MSG_DELETE_PREFIX = '$JS.API.STREAM.MSG.DELETE.';
    public const CONSUMER_CREATE_PREFIX = '$JS.API.CONSUMER.CREATE.';
    public const CONSUMER_INFO_PREFIX = '$JS.API.CONSUMER.INFO.';
    public const CONSUMER_DELETE_PREFIX = '$JS.API.CONSUMER.DELETE.';
    public const CONSUMER_PAUSE_PREFIX = '$JS.API.CONSUMER.PAUSE.';
    public const CONSUMER_UNPIN_PREFIX = '$JS.API.CONSUMER.UNPIN.';
    public const CONSUMER_LIST_PREFIX = '$JS.API.CONSUMER.LIST.';
    public const CONSUMER_MSG_NEXT_PREFIX = '$JS.API.CONSUMER.MSG.NEXT.';
    public const STREAM_PURGE_PREFIX = '$JS.API.STREAM.PURGE.';
    public const STREAM_LIST = '$JS.API.STREAM.LIST';
    public const STREAM_NAMES = '$JS.API.STREAM.NAMES';
    public const CONSUMER_NAMES_PREFIX = '$JS.API.CONSUMER.NAMES.';
    public const STREAM_DIRECT_GET_PREFIX = '$JS.API.DIRECT.GET.';

    /**
     * Returns the API prefix of a JetStream domain, `$JS.<domain>.API.` (nats.go `jetstream.NewWithDomain()`, nats.java
     * `JetStreamOptions.domain()`). A domain is a single subject token: nats-server refuses a domain with a `.`, `*`,
     * `>` or whitespace, and so does this method, before any request is sent.
     *
     * @throws JetStreamException When the domain is empty or contains whitespace, a control character, `.`, `*` or `>`.
     */
    public static function domainPrefix(string $domain): string
    {
        if ($domain === '') {
            throw new JetStreamException('JetStream domain must not be empty');
        }

        if (preg_match('/[\x00-\x20\x7f.*>]/', $domain) === 1) {
            throw new JetStreamException(
                'Invalid JetStream domain "' . $domain . '": must not contain whitespace, control characters, ".", "*" or ">"',
            );
        }

        return '$JS.' . $domain . '.API.';
    }

    /**
     * Normalizes an API prefix given directly, such as the subject an account imports another account's JetStream API
     * under (nats.go `jetstream.NewWithAPIPrefix()`, nats.java `JetStreamOptions.prefix()`): a trailing `.` is added
     * when missing (`JS.from-a.API` becomes `JS.from-a.API.`).
     *
     * @throws JetStreamException When the prefix is empty, has an empty token (`a..b`, a leading `.`), or contains
     *                            whitespace, a control character, `*` or `>`.
     */
    public static function normalizePrefix(string $prefix): string
    {
        $body = str_ends_with($prefix, '.') ? substr($prefix, 0, -1) : $prefix;
        if ($body === '') {
            throw new JetStreamException('JetStream API prefix must not be empty');
        }

        if (preg_match('/[\x00-\x20\x7f*>]/', $body) === 1 || in_array('', explode('.', $body), true)) {
            throw new JetStreamException(
                'Invalid JetStream API prefix "' . $prefix . '": must be a subject of non-empty tokens without '
                . 'whitespace, control characters, "*" or ">"',
            );
        }

        return $body . '.';
    }

    /**
     * Resolves the API prefix of a context from an optional domain and an optional API prefix: the domain's prefix
     * ({@see domainPrefix()}), the given prefix ({@see normalizePrefix()}), or {@see DEFAULT_PREFIX} when neither is
     * given. A domain and a prefix together are refused, as nats.go refuses them.
     *
     * @throws JetStreamException When both are given, or the one given is invalid.
     */
    public static function resolvePrefix(?string $domain, ?string $apiPrefix): string
    {
        if ($domain !== null && $apiPrefix !== null) {
            throw new JetStreamException('Set either a JetStream domain or a JetStream API prefix, not both');
        }

        if ($domain !== null) {
            return self::domainPrefix($domain);
        }

        if ($apiPrefix !== null) {
            return self::normalizePrefix($apiPrefix);
        }

        return self::DEFAULT_PREFIX;
    }

    /**
     * Rebases a JetStream API subject (one of the constants above, plus its tokens) onto $prefix: the leading
     * {@see DEFAULT_PREFIX} is replaced by $prefix, which must already be normalized (end with `.`). A subject that
     * does not start with {@see DEFAULT_PREFIX} is returned unchanged.
     */
    public static function withPrefix(string $subject, string $prefix): string
    {
        if ($prefix === self::DEFAULT_PREFIX || !str_starts_with($subject, self::DEFAULT_PREFIX)) {
            return $subject;
        }

        return $prefix . substr($subject, strlen(self::DEFAULT_PREFIX));
    }
}
