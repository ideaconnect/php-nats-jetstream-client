<?php

declare(strict_types=1);

namespace IDCT\NATS\Connection\Enum;

enum ConnectionState: string
{
    case Idle = 'idle';
    case Connecting = 'connecting';
    case Open = 'open';
    case Draining = 'draining';
    case Closed = 'closed';
}
