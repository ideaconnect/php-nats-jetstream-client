<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Enum;

enum AckPolicy: string
{
    case None = 'none';
    case All = 'all';
    case Explicit = 'explicit';

    /**
     * NATS server 2.14+ (ADR-60, durable stream sourcing): the policy of the durable push consumer that a stream
     * sources or mirrors another stream through, named in the source with
     * {@see \IDCT\NATS\JetStream\Configuration\StreamSource::consumer()}. The sourcing stream acknowledges through
     * flow control what it has stored, so a WorkQueue or Interest origin removes a message only after that.
     *
     * A durable name, a deliver subject and this policy are enough: the server sets `flow_control: true` and a 1 s
     * `idle_heartbeat` itself (overwriting other values without an error), and its reply carries
     * `max_ack_pending: 1000` and `max_deliver: -1` when the request sets neither. It refuses such a consumer with
     * `ack_wait` or `backoff` (err_code 10221), a positive `max_deliver` (10222), no deliver subject (10218),
     * `max_ack_pending: -1` (10220), and a deliver subject inside the origin stream's own subjects (10081).
     * A server before 2.14 refuses the policy itself (400, err_code 10025, `invalid JSON`).
     */
    case FlowControl = 'flow_control';
}
