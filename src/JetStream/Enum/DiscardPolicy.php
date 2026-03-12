<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Enum;

enum DiscardPolicy: string
{
    case Old = 'old';
    case New = 'new';
}
