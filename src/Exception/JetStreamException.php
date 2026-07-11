<?php

declare(strict_types=1);

namespace IDCT\NATS\Exception;

/**
 * Thrown when JetStream API responses indicate an application-level failure.
 *
 * Not final so it can be specialized - see {@see UnsupportedFeatureException}, which is raised when a
 * request fails because the connected server is too old for a feature. Existing `catch
 * (JetStreamException)` handlers still catch those.
 */
class JetStreamException extends NatsException
{
    /**
     * @param int      $code    The API error envelope's HTTP-like `code` (400/404/503, or a pull/direct
     *                          status), or 0 for client-side errors.
     * @param int|null $errCode The envelope's stable `err_code` (ADR-1; e.g. 10058 "stream name already
     *                          in use"), or null when the envelope carried none or the error is
     *                          client-side. Discriminate error kinds on this, not on message wording.
     */
    public function __construct(
        string $message = '',
        int $code = 0,
        ?\Throwable $previous = null,
        private readonly ?int $errCode = null,
    ) {
        parent::__construct($message, $code, $previous);
    }

    /**
     * The JetStream API error number (`err_code`) from the server's error envelope, or null when
     * unavailable.
     */
    public function getErrCode(): ?int
    {
        return $this->errCode;
    }
}
