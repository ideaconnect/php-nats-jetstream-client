<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\Configuration\StreamSource;
use PHPUnit\Framework\Attributes\DataProvider;
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

    public function testSourceWithConsumer(): void
    {
        $source = StreamSource::source('WQ')
            ->consumer('SRC', 'deliver.x')
            ->toArray();

        self::assertSame([
            'name' => 'WQ',
            'consumer' => ['name' => 'SRC', 'deliver_subject' => 'deliver.x'],
        ], $source);
    }

    public function testMirrorWithConsumer(): void
    {
        $mirror = StreamSource::mirror('WQ')
            ->consumer('SRC', 'deliver.x')
            ->toArray();

        self::assertSame([
            'name' => 'WQ',
            'consumer' => ['name' => 'SRC', 'deliver_subject' => 'deliver.x'],
        ], $mirror);
    }

    public function testConsumerComesAfterExternal(): void
    {
        $source = StreamSource::source('WQ')
            ->consumer('SRC', 'deliver.x')
            ->external('$JS.hub.API', '_DELIVER.hub')
            ->toArray();

        self::assertSame([
            'name' => 'WQ',
            'external' => ['api' => '$JS.hub.API', 'deliver' => '_DELIVER.hub'],
            'consumer' => ['name' => 'SRC', 'deliver_subject' => 'deliver.x'],
        ], $source);
    }

    /**
     * @return iterable<string, array{\Closure(StreamSource): StreamSource, bool, string}>
     */
    public static function startOrFilterSetters(): iterable
    {
        $startSeq = static fn(StreamSource $source): StreamSource => $source->startSeq(5);
        $startTime = static fn(StreamSource $source): StreamSource => $source->startTime('2026-01-01T00:00:00Z');
        $filterSubject = static fn(StreamSource $source): StreamSource => $source->filterSubject('wq.a');

        yield 'startSeq() before consumer()' => [$startSeq, true, 'startSeq()'];
        yield 'startSeq() after consumer()' => [$startSeq, false, 'startSeq()'];
        yield 'startTime() before consumer()' => [$startTime, true, 'startTime()'];
        yield 'startTime() after consumer()' => [$startTime, false, 'startTime()'];
        yield 'filterSubject() before consumer()' => [$filterSubject, true, 'filterSubject()'];
        yield 'filterSubject() after consumer()' => [$filterSubject, false, 'filterSubject()'];
    }

    /**
     * @param \Closure(StreamSource): StreamSource $setter
     */
    #[DataProvider('startOrFilterSetters')]
    public function testConsumerCannotBeCombinedWithAStartOrAFilter(\Closure $setter, bool $setterFirst, string $setterName): void
    {
        $source = StreamSource::source('WQ');
        if ($setterFirst) {
            $setter($source)->consumer('SRC', 'deliver.x');
        } else {
            $setter($source->consumer('SRC', 'deliver.x'));
        }

        try {
            $source->toArray();
            self::fail('expected toArray() to refuse consumer() together with ' . $setterName);
        } catch (JetStreamException $e) {
            self::assertSame(
                'Stream source "WQ": consumer() cannot be combined with ' . $setterName
                . '; set the start and the filter on the consumer "SRC" instead',
                $e->getMessage(),
            );
        }
    }

    public function testConsumerClashNamesEverySetterThatClashes(): void
    {
        $source = StreamSource::mirror('WQ')
            ->filterSubject('wq.a')
            ->startTime('2026-01-01T00:00:00Z')
            ->startSeq(5)
            ->consumer('SRC', 'deliver.x');

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('consumer() cannot be combined with startSeq(), startTime(), filterSubject();');

        $source->toArray();
    }

    public function testConsumerNeedsAName(): void
    {
        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('A stream source consumer needs a name');

        StreamSource::source('WQ')->consumer('', 'deliver.x');
    }

    public function testConsumerNeedsADeliverSubject(): void
    {
        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('A stream source consumer needs a deliver subject');

        StreamSource::source('WQ')->consumer('SRC', '');
    }
}
