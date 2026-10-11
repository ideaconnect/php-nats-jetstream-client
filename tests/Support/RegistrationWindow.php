<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

/**
 * What a {@see LoopRegistrationCounter} counted while this window was open (#184): every callback registered on the event
 * loop - defer, delay, repeat, readable, writable, signal - in all, and by kind. Once the window is closed it keeps its
 * totals and counts nothing more.
 */
final class RegistrationWindow
{
    /** Every registration counted while the window was open. */
    public int $registrations = 0;

    /** @var array<string, int> The same, by kind ("defer", "delay", "repeat", "readable", "writable", "signal"). */
    public array $kinds = [];
}
