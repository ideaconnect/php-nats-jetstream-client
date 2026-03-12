<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\JetStream\Configuration\StreamSource;
use PHPUnit\Framework\TestCase;

final class StreamSourceTest extends TestCase
{
    public function testMirrorMinimal(): void
    {
        $mirror = StreamSource::mirror('ORIGIN')->toArray();

        self::assertSame(['name' => 'ORIGIN'], $mirror);
    }

    public function testMirrorWithStartSeq(): void
    {
        $mirror = StreamSource::mirror('ORIGIN')
            ->startSeq(42)
            ->toArray();

        self::assertSame('ORIGIN', $mirror['name']);
        self::assertSame(42, $mirror['opt_start_seq']);
    }

    public function testMirrorWithStartTime(): void
    {
        $mirror = StreamSource::mirror('ORIGIN')
            ->startTime('2026-01-01T00:00:00Z')
            ->toArray();

        self::assertSame('2026-01-01T00:00:00Z', $mirror['opt_start_time']);
    }

    public function testSourceWithFilterSubject(): void
    {
        $source = StreamSource::source('ORDERS')
            ->filterSubject('orders.>')
            ->toArray();

        self::assertSame('ORDERS', $source['name']);
        self::assertSame('orders.>', $source['filter_subject']);
    }

    public function testSourceWithExternal(): void
    {
        $source = StreamSource::source('REMOTE')
            ->external('$JS.other.API', '_DELIVER.other')
            ->toArray();

        self::assertSame('REMOTE', $source['name']);
        self::assertSame(['api' => '$JS.other.API', 'deliver' => '_DELIVER.other'], $source['external']);
    }

    public function testExternalWithoutDeliver(): void
    {
        $source = StreamSource::source('REMOTE')
            ->external('$JS.other.API')
            ->toArray();

        self::assertSame(['api' => '$JS.other.API'], $source['external']);
    }

    public function testFullyConfiguredSource(): void
    {
        $source = StreamSource::source('EVENTS')
            ->startSeq(100)
            ->filterSubject('events.>')
            ->external('$JS.X.API', '_D.X')
            ->toArray();

        self::assertSame([
            'name' => 'EVENTS',
            'opt_start_seq' => 100,
            'filter_subject' => 'events.>',
            'external' => ['api' => '$JS.X.API', 'deliver' => '_D.X'],
        ], $source);
    }
}
