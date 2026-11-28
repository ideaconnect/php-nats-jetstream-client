<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream;

use DateTimeImmutable;
use DateTimeInterface;
use DateTimeZone;

/**
 * Helper for building scheduler expressions (ADR-51) for {@see JetStreamContext::publishScheduled()}.
 *
 * Server versions: a one-off `@at` schedule ({@see at()}, {@see atTimestamp()}) needs NATS server 2.12+;
 * the recurring forms ({@see every()}, {@see cron()}, {@see predefined()}) need 2.14+ on the servers that
 * hold the stream. `publishScheduled()` sends every schedule whatever version the connected server reports
 * (ADR-44, #272), and the server that stores it decides: a 2.12 server answers a recurring schedule with its
 * own 400 (`message schedules pattern is invalid`), thrown as a {@see \IDCT\NATS\Exception\JetStreamException}.
 */
final class Schedule
{
    /**
     * Formats a one-off "@at <RFC3339 UTC>" schedule expression. Needs NATS server 2.12+.
     */
    public static function at(DateTimeInterface $when): string
    {
        $utc = new DateTimeZone('UTC');
        $value = DateTimeImmutable::createFromInterface($when)->setTimezone($utc);

        return '@at ' . $value->format('Y-m-d\TH:i:s\Z');
    }

    /**
     * Formats a one-off "@at" schedule expression from a Unix timestamp. Needs NATS server 2.12+.
     */
    public static function atTimestamp(int $timestamp): string
    {
        $utc = new DateTimeZone('UTC');
        $when = (new DateTimeImmutable('@' . $timestamp))->setTimezone($utc);

        return self::at($when);
    }

    /**
     * Formats a recurring "@every <interval>" schedule. Accepts an integer number of seconds or a
     * Go-style duration string (e.g. "1h", "30m", "1h30m"). The server applies the schedule starting
     * from the publish time and re-delivers on each interval; it rejects intervals shorter than 1s.
     * Needs NATS server 2.14+ (2.12 accepts only "@at").
     */
    public static function every(int|string $interval): string
    {
        if (is_int($interval)) {
            if ($interval <= 0) {
                throw new \InvalidArgumentException('Schedule interval must be a positive number of seconds');
            }

            return '@every ' . $interval . 's';
        }

        $interval = trim($interval);
        if ($interval === '') {
            throw new \InvalidArgumentException('Schedule interval must not be empty');
        }

        return '@every ' . $interval;
    }

    /**
     * Validates and returns a 6-field (seconds-resolution) cron schedule expression as accepted by
     * the NATS scheduler, e.g. "0 0 * * * *" (every hour on the hour). The fields are
     * second minute hour day-of-month month day-of-week. Needs NATS server 2.14+ (2.12 accepts only
     * "@at"), as does the time zone a cron schedule may carry.
     */
    public static function cron(string $expression): string
    {
        $expression = trim($expression);
        $fields = $expression === '' ? [] : preg_split('/\s+/', $expression);

        if ($fields === false || count($fields) !== 6) {
            throw new \InvalidArgumentException(
                'Cron expression must have 6 space-separated fields (second minute hour day-of-month month day-of-week)',
            );
        }

        return $expression;
    }

    /**
     * Returns one of the predefined NATS scheduler aliases (ADR-51): hourly, daily, weekly, monthly,
     * yearly, annually, or midnight (with or without a leading "@"). These are cron-class schedules
     * and may carry a time zone. Needs NATS server 2.14+ (2.12 accepts only "@at").
     */
    public static function predefined(string $alias): string
    {
        $normalized = '@' . ltrim(strtolower(trim($alias)), '@');

        if (preg_match('/^@(?:hourly|daily|weekly|monthly|yearly|annually|midnight)$/', $normalized) !== 1) {
            throw new \InvalidArgumentException(
                'Unknown schedule alias "' . $alias . '": expected hourly, daily, weekly, monthly, yearly, annually, or midnight',
            );
        }

        return $normalized;
    }
}
