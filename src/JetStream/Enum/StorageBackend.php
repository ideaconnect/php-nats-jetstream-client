<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Enum;

enum StorageBackend: string
{
    case File = 'file';
    case Memory = 'memory';
}
