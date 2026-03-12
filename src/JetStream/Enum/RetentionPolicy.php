<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Enum;

enum RetentionPolicy: string
{
    case Limits = 'limits';
    case Interest = 'interest';
    case WorkQueue = 'workqueue';
}
