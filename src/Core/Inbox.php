<?php

declare(strict_types=1);

namespace IDCT\NATS\Core;

final class Inbox
{
    /**
     * Generates a unique inbox subject used for request/reply subscriptions.
     */
    public static function generate(string $prefix = '_INBOX'): string
    {
        return $prefix . '.' . bin2hex(random_bytes(12));
    }
}
