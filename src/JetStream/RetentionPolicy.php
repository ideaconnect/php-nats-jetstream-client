<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream;

enum RetentionPolicy: string
{
    case Limits = 'limits';
    case Interest = 'interest';
    case WorkQueue = 'workqueue';
}
