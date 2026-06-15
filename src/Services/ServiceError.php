<?php

declare(strict_types=1);

namespace IDCT\NATS\Services;

/**
 * Thrown by a service endpoint handler to reply with an explicit micro-spec error - a chosen code and
 * description (and optional body) - instead of the generic internal-error response. The runtime emits
 * the `Nats-Service-Error` / `Nats-Service-Error-Code` reply headers so a generic micro client detects
 * the failure without parsing the body. Mirrors nats.go `Request.Error()` / nats.java `respondStandardError`.
 */
final class ServiceError extends \RuntimeException
{
    public readonly string $serviceErrorCode;
    public readonly string $description;
    public readonly ?string $body;

    /**
     * @param int|string  $code        Micro error code surfaced as `Nats-Service-Error-Code` (e.g. "400").
     * @param string      $description Human-readable error surfaced as `Nats-Service-Error`.
     * @param string|null $body        Optional response body sent alongside the error headers. When null,
     *                                 a JSON micro error payload is sent.
     */
    public function __construct(int|string $code, string $description, ?string $body = null)
    {
        $this->serviceErrorCode = (string) $code;
        $this->description = $description;
        $this->body = $body;

        parent::__construct($description);
    }
}
