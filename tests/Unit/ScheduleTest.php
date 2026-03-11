<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use DateTimeImmutable;
use DateTimeZone;
use IDCT\NATS\JetStream\Schedule;
use PHPUnit\Framework\TestCase;

final class ScheduleTest extends TestCase
{
    /**
     * Verifies helper formats @at schedules in UTC.
     */
    public function testAtFormatsUtcExpression(): void
    {
        $when = new DateTimeImmutable('2030-01-01 00:00:00', new DateTimeZone('UTC'));

        $schedule = Schedule::at($when);

        self::assertSame('@at 2030-01-01T00:00:00Z', $schedule);
    }

    /**
     * Verifies helper normalizes non-UTC input to UTC schedule format.
     */
    public function testAtNormalizesTimezoneToUtc(): void
    {
        $when = new DateTimeImmutable('2030-01-01 01:00:00', new DateTimeZone('Europe/Warsaw'));

        $schedule = Schedule::at($when);

        self::assertSame('@at 2030-01-01T00:00:00Z', $schedule);
    }

    /**
     * Verifies Unix timestamp helper returns valid @at expression.
     */
    public function testAtTimestamp(): void
    {
        $schedule = Schedule::atTimestamp(1_893_456_000);

        self::assertSame('@at 2030-01-01T00:00:00Z', $schedule);
    }
}
