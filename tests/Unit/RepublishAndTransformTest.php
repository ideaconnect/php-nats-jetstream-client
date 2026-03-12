<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\JetStream\Configuration\Republish;
use IDCT\NATS\JetStream\Configuration\SubjectTransform;
use PHPUnit\Framework\TestCase;

final class RepublishAndTransformTest extends TestCase
{
    public function testRepublishMinimal(): void
    {
        $config = Republish::create('orders.>', 'copy.orders.>')->toArray();

        self::assertSame([
            'src' => 'orders.>',
            'dest' => 'copy.orders.>',
        ], $config);
    }

    public function testRepublishHeadersOnly(): void
    {
        $config = Republish::create('orders.>', 'notify.orders.>')
            ->headersOnly()
            ->toArray();

        self::assertSame([
            'src' => 'orders.>',
            'dest' => 'notify.orders.>',
            'headers_only' => true,
        ], $config);
    }

    public function testRepublishHeadersOnlyFalse(): void
    {
        $config = Republish::create('a', 'b')
            ->headersOnly(true)
            ->headersOnly(false)
            ->toArray();

        self::assertSame(['src' => 'a', 'dest' => 'b'], $config);
    }

    public function testSubjectTransform(): void
    {
        $config = SubjectTransform::create('raw.>', 'mapped.>')->toArray();

        self::assertSame([
            'src' => 'raw.>',
            'dest' => 'mapped.>',
        ], $config);
    }

    public function testSubjectTransformWithTokenMapping(): void
    {
        $config = SubjectTransform::create('input.*.data', 'output.$1.processed')->toArray();

        self::assertSame('input.*.data', $config['src']);
        self::assertSame('output.$1.processed', $config['dest']);
    }
}
