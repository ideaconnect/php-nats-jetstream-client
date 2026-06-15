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
class JetStreamException extends NatsException {}
