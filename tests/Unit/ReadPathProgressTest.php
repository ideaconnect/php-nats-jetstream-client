<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\IncomingChunkResult;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Core\SubscriptionQueue;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\LoopTickCountingTransport;
use IDCT\NATS\Transport\TransportInterface;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\Future\await;

/**
 * Read-path progress accounting (#119): a wait loop must distinguish "made parse progress / consumed
 * bytes this read" from "truly idle (empty read)". A frame spanning N transport chunks must NOT pay a
 * 1 ms idle sleep per chunk while its bytes are already arriving, but a genuinely idle wait must still
 * honor its deadline and not busy-spin.
 */
final class ReadPathProgressTest extends TestCase
{
    /** @return list<string> */
    private function infoAndPong(): array
    {
        return [
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ];
    }

    private function makeConnectedClient(TransportInterface $transport): NatsClient
    {
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        return $client;
    }

    /**
     * The core signal: a read that pulls bytes off the wire reports consumedBytes=true even when the
     * frame is not yet complete (frames=0), and only a genuinely empty read reports consumedBytes=false.
     * Also proves the multi-chunk frame is reassembled and delivered byte-for-byte.
     *
     * FALSIFIABILITY: on the pre-#119 read path, a partial-frame read reports only a 0 frame count with
     * no "made progress" bit, so the wait loops sleep 1 ms per chunk; this test asserts the bit is set
     * on every byte-consuming read.
     */
    public function testReadIncomingReportsConsumedBytesOnPartialFrameAndIdleOnEmptyRead(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = $this->makeConnectedClient($transport);

        /** @var list<NatsMessage> $received */
        $received = [];
        $sid = $client->subscribe('big', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message;
        })->await();

        $payload = str_repeat('AB', 25); // 50 bytes
        $frame = 'MSG big ' . $sid . ' ' . strlen($payload) . "\r\n" . $payload . "\r\n";

        // Feed the frame one byte at a time: every read but the last consumes a byte WITHOUT
        // completing the frame (partial progress), and the last read completes it.
        $bytes = str_split($frame);
        foreach ($bytes as $byte) {
            $transport->pushReadChunk($byte);
        }

        $partialProgressReads = 0;
        $completedFrames = 0;
        foreach ($bytes as $_) {
            $read = $client->readIncoming()->await();
            self::assertInstanceOf(IncomingChunkResult::class, $read);
            self::assertTrue(
                $read->consumedBytes,
                'a read that pulled a byte off the wire must report consumedBytes=true even mid-frame',
            );
            if ($read->frames === 0) {
                $partialProgressReads++;
            } else {
                $completedFrames += $read->frames;
            }
        }

        // Exactly one read (the last byte) completed the frame; every earlier read made partial
        // progress with 0 frames - the reads a wait loop must NOT idle-sleep on (#119).
        self::assertSame(count($bytes) - 1, $partialProgressReads);
        self::assertSame(1, $completedFrames);

        // The queue is now empty: a genuinely idle read consumes nothing. This is the ONLY case a
        // wait loop should yield 1 ms.
        $idle = $client->readIncoming()->await();
        self::assertSame(0, $idle->frames);
        self::assertFalse($idle->consumedBytes, 'an empty read must report consumedBytes=false so the loop yields');

        // The multi-chunk frame was reassembled byte-for-byte.
        self::assertCount(1, $received);
        self::assertSame($payload, $received[0]->payload);
    }

    /**
     * The public {@see NatsClient::processIncoming()} frame-count contract is unchanged by the #119
     * refactor: a message split across chunks still parses to exactly one frame, delivered identically.
     */
    public function testProcessIncomingStillReturnsFrameCountForMultiChunkMessage(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = $this->makeConnectedClient($transport);

        /** @var list<NatsMessage> $received */
        $received = [];
        $sid = $client->subscribe('big', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message;
        })->await();

        $payload = str_repeat('payload-', 200); // 1600 bytes
        $frame = 'MSG big ' . $sid . ' ' . strlen($payload) . "\r\n" . $payload . "\r\n";
        $chunks = str_split($frame, 7); // 7-byte chunks: many partial reads before completion
        foreach ($chunks as $chunk) {
            $transport->pushReadChunk($chunk);
        }

        $totalFrames = 0;
        foreach ($chunks as $_) {
            $totalFrames += $client->processIncoming()->await();
        }

        self::assertSame(1, $totalFrames, 'the chunked message must parse to exactly one frame');
        self::assertCount(1, $received);
        self::assertSame($payload, $received[0]->payload);
    }

    /**
     * End-to-end proof through a real wait loop ({@see SubscriptionQueue::fetchAll()}): a message split
     * into many partial chunks is collected with no pause between its reads, because partial-progress
     * reads loop immediately instead of idle-sleeping.
     *
     * FALSIFIABILITY: the frame is split into 317 one-byte chunks, and the transport counts the chunks
     * read after the event loop ran a tick since the chunk before ({@see LoopTickCountingTransport}). On
     * the pre-#119 path each of the ~316 partial reads sleeps 1 ms, and the loop ticks during each sleep:
     * every chunk after the first counts. With the fix the reads follow each other without a tick and the
     * count is 0. The count does not depend on how fast the machine is, and the 5 s window gives the fixed
     * path all the time it needs to collect the message (#176).
     */
    public function testChunkedMessageIsCollectedWithoutIdleSleepPerChunk(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $counting = new LoopTickCountingTransport($transport);
        $client = $this->makeConnectedClient($counting);

        $queue = $client->subscribeQueue('big')->await();
        self::assertInstanceOf(SubscriptionQueue::class, $queue);

        $payload = str_repeat('x', 300);
        $frame = 'MSG big ' . $queue->sid . ' ' . strlen($payload) . "\r\n" . $payload . "\r\n";
        $bytes = str_split($frame); // 317 one-byte chunks
        foreach ($bytes as $byte) {
            $transport->pushReadChunk($byte);
        }

        $queue->setTimeout(5.0);
        $counting->reset(); // count the frame's chunks only, not the handshake's

        // Outer bound: fail loudly rather than hang if a regression parks the caller.
        $messages = await([async(static fn (): array => $queue->fetchAll(1))], new TimeoutCancellation(10.0))[0];

        self::assertCount(1, $messages, 'the chunked message must be collected');
        self::assertSame($payload, $messages[0]->payload);
        self::assertSame(count($bytes), $counting->chunksRead, 'every byte of the frame must be read as its own chunk');
        // Only a loop that sleeps 1 ms per partial read lets the event loop tick between two of them.
        self::assertSame(
            0,
            $counting->chunksReadAfterALoopTick,
            'collection must read the next chunk at once, not accrue a 1 ms sleep per chunk',
        );
    }

    /**
     * A genuinely idle wait still honors its deadline and does not busy-spin: with a live-but-silent
     * socket (blockWhenEmpty), next() with a 50 ms timeout returns null at ~the deadline - it neither
     * bails early (proving the read is bounded) nor hangs (proving the deadline fires).
     */
    public function testIdleWaitRespectsDeadlineAndDoesNotBusySpin(): void
    {
        $transport = new FakeTransport($this->infoAndPong(), blockWhenEmpty: true);
        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('idle.subject')->await();
        $queue->setTimeout(0.05);

        $startedNs = hrtime(true);
        $message = await([async(static fn (): ?NatsMessage => $queue->next())], new TimeoutCancellation(5.0))[0];
        $elapsedSeconds = (hrtime(true) - $startedNs) / 1e9;

        self::assertNull($message, 'an idle wait returns null at its deadline');
        self::assertGreaterThanOrEqual(0.045, $elapsedSeconds, 'the wait must honor its full deadline, not return early');
        self::assertLessThan(2.0, $elapsedSeconds, 'the wait must terminate at its deadline, not hang');
        self::assertTrue($transport->lastReadHadCancellation, 'the idle read must be bounded by a cancellation');
    }
}
