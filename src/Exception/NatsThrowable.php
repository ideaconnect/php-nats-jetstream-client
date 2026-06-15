<?php

declare(strict_types=1);

namespace IDCT\NATS\Exception;

use Throwable;

/**
 * Marker interface implemented by every exception this library throws - both the {@see NatsException}
 * hierarchy and the transport-layer exceptions ({@see \IDCT\NATS\Transport\TransportClosedException}
 * and {@see \IDCT\NATS\Transport\TlsRequiredException}) which extend \RuntimeException rather than
 * NatsException so the connection layer's catch (\Throwable) recovery paths still handle them.
 *
 * Catch this interface to handle any error originating from this library in a single clause, without
 * also catching unrelated \RuntimeExceptions (#91).
 */
interface NatsThrowable extends Throwable {}
