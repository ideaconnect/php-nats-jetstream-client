<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream;

enum StorageBackend: string
{
    case File = 'file';
    case Memory = 'memory';
}
