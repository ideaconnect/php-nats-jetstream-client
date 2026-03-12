<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Enum;

enum ReplayPolicy: string
{
    case Instant = 'instant';
    case Original = 'original';
}
