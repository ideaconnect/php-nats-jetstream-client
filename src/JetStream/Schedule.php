<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream;

use DateTimeImmutable;
use DateTimeInterface;
use DateTimeZone;

/**
 * Helper for building scheduler expressions accepted by current NATS behavior.
 */
final class Schedule
{
    /**
     * Formats a schedule expression accepted by current NATS scheduler behavior.
     */
    public static function at(DateTimeInterface $when): string
    {
        $utc = new DateTimeZone('UTC');
        $value = DateTimeImmutable::createFromInterface($when)->setTimezone($utc);

        return '@at ' . $value->format('Y-m-d\TH:i:s\Z');
    }

    /**
     * Formats a schedule expression from a Unix timestamp.
     */
    public static function atTimestamp(int $timestamp): string
    {
        $utc = new DateTimeZone('UTC');
        $when = (new DateTimeImmutable('@' . $timestamp))->setTimezone($utc);

        return self::at($when);
    }
}
