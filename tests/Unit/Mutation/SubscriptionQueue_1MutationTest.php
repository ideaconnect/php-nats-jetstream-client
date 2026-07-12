<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Core\SubscriptionQueue;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\Future\await;

/**
 * Kills the surviving Infection mutants in {@see SubscriptionQueue::next()}'s bounded wait loop
 * (the #119 read-path change), lines 208 and 212:
 *
 *   do {
 *       $read = $this->client->readIncoming($cancellation)->await();
 *       if ($this->messages->count() > 0) { break; }
 *       if (!$read->consumedBytes) {          // <- LogicalNot @ 208
 *           delay(0.001, cancellation: $cancellation);  // <- FunctionCallRemoval @ 212
 *       }
 *   } while ($this->monotonicSeconds() < $deadline);
 *
 * The two mutants both concern *when* the loop pays the 1 ms idle yield. Each test picks a timeout
 * window in which the real behavior and the mutated behavior produce a different next() return value
 * (a message vs null), so the difference is a crisp, deterministic assertion rather than a timing bound.
 */
final class SubscriptionQueue_1MutationTest extends TestCase
{
    /** @return list<string> */
    private function infoAndPong(): array
    {
        return [
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ];
    }

    private function makeConnectedClient(FakeTransport $transport): NatsClient
    {
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        return $client;
    }

    /**
     * kills LogicalNot @ line 208 (`if (!$read->consumedBytes)` -> `if ($read->consumedBytes)`).
     *
     * A single frame is fed as many one-byte partial chunks: every read pulls a byte (consumedBytes
     * = true) but completes no frame until the last byte lands. On the REAL code `!consumedBytes` is
     * false for each partial read, so the loop drains all ~317 chunks with NO per-chunk sleep and
     * assembles the message well inside the 60 ms window -> next() returns it.
     *
     * The mutant inverts the guard to `if ($read->consumedBytes)`, so it sleeps 1 ms on EVERY
     * partial read (~316 ms of accrued sleeps). The TimeoutCancellation fires mid-frame, the message
     * is never completed, and next() returns null. assertNotNull therefore fails on the mutant.
     */
    public function testNextAssemblesChunkedFrameWithoutIdleSleepPerPartialChunk(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = $this->makeConnectedClient($transport);

        $queue = $client->subscribeQueue('big')->await();
        self::assertInstanceOf(SubscriptionQueue::class, $queue);

        $payload = str_repeat('x', 300);
        $frame = 'MSG big ' . $queue->sid . ' ' . strlen($payload) . "\r\n" . $payload . "\r\n";
        foreach (str_split($frame) as $byte) { // 317 one-byte partial chunks
            $transport->pushReadChunk($byte);
        }

        // ~316 partial reads would cost ~316 ms of 1 ms sleeps on the mutant; the real path pays none.
        $queue->setTimeout(0.06);

        $startedNs = hrtime(true);
        // Outer bound: fail loudly rather than hang if a regression parks the caller.
        $message = await([async(static fn (): ?NatsMessage => $queue->next())], new TimeoutCancellation(5.0))[0];
        $elapsedSeconds = (hrtime(true) - $startedNs) / 1e9;

        // kills LogicalNot @ 208: real code assembles + returns the message; the mutant times out to null.
        self::assertNotNull($message, 'partial-frame reads must loop without a per-chunk idle sleep');
        self::assertSame($payload, $message->payload);
        // Real code finishes near-instantly, not after the ~316 ms a per-partial-chunk sleep would cost.
        self::assertLessThan(0.06, $elapsedSeconds, 'the frame must assemble inside the window, not sleep per partial chunk');
    }

    /**
     * kills FunctionCallRemoval @ line 212 (removing `delay(0.001, cancellation: $cancellation)`).
     *
     * The queue is fed 150 genuinely idle reads (empty chunks -> consumedBytes = false) followed by a
     * real message. On the REAL code each idle read yields 1 ms, so within the 50 ms window at most
     * ~50 idle reads are consumed - the message at position 150 is never reached and next() returns
     * null (the wait honors its deadline instead of racing ahead).
     *
     * With the delay removed the loop busy-spins: it drains all 150 idle reads and the trailing
     * message in well under the window, enqueues it, breaks, and next() returns the message. So
     * assertNull fails on the mutant. (This also kills the LogicalNot mutant, which likewise skips
     * the yield on these idle reads and races to the message.)
     */
    public function testNextIdleWaitYieldsPerEmptyReadInsteadOfBusySpinning(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = $this->makeConnectedClient($transport);

        $queue = $client->subscribeQueue('idle.then.msg')->await();
        self::assertInstanceOf(SubscriptionQueue::class, $queue);

        for ($i = 0; $i < 150; $i++) {
            $transport->pushReadChunk(''); // genuinely idle read: consumedBytes = false
        }
        // The message sits far past what a 1 ms-per-idle-read wait can reach inside a 50 ms window.
        $transport->pushReadChunk('MSG idle.then.msg ' . $queue->sid . " 9\r\nlate-here\r\n");

        // 50 ms window: the real path (>=1 ms per idle read) reaches at most ~50 of the 150 idle reads.
        $queue->setTimeout(0.05);

        // Outer bound: fail loudly rather than hang if a regression parks the caller.
        $message = await([async(static fn (): ?NatsMessage => $queue->next())], new TimeoutCancellation(5.0))[0];

        // kills FunctionCallRemoval @ 212: real code idle-yields and times out to null; the mutant
        // busy-spins past every idle read to the trailing message and returns it.
        self::assertNull($message, 'an idle wait must yield per empty read and honor its deadline, not busy-spin to a late message');
    }
}
