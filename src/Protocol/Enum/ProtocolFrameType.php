<?php

declare(strict_types=1);

namespace IDCT\NATS\Protocol\Enum;

enum ProtocolFrameType: string
{
    case Info = 'INFO';
    case Ping = 'PING';
    case Pong = 'PONG';
    case Ok = '+OK';
    case Err = '-ERR';
    case Msg = 'MSG';
    case HMsg = 'HMSG';
}
