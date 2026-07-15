<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

/**
 * Kills the two #119 read-path survivors in KeyValueBucket:
 *   - LogicalNot @ 616 (keys(): `!$read->consumedBytes` -> `$read->consumedBytes`)
 *   - UnwrapFinally @ 711 (history(): the try/finally that guarantees the deliver unsubscribe)
 */
final class KeyValueBucket_4MutationTest extends TestCase
{
    /**
     * A KV replay can leave a live TimeoutCancellation / delay() timer on the loop; cancel every
     * registered callback before each test so a leaked timer cannot fire into a later test's fiber.
     */
    protected function setUp(): void
    {
        foreach (EventLoop::getIdentifiers() as $id) {
            EventLoop::cancel($id);
        }
    }

    /**
     * kills LogicalNot @ line 616.
     *
     * keys() drains a single key-record HMSG frame that arrives as ~360 one-byte transport chunks.
     * REAL code: every byte-consuming read reports consumedBytes=true, so `!$read->consumedBytes` is
     * false and NO 1 ms idle sleep is paid per chunk (#119) - the whole frame drains near-instantly,
     * well inside the 0.2 s progress bound, so keys() returns ['email'].
     *
     * MUTANT (`if ($read->consumedBytes)`): every partial-frame read now DOES sleep 1 ms. With ~360
     * one-byte chunks the accrued idle sleeps blow past the 0.2 s bound before the frame ever completes,
     * so the progress deadline fires mid-frame and keys() throws a JetStreamException instead of
     * returning - the assertSame() below then fails, killing the mutant. (A tight per-record timeout is
     * used so the mutant's per-chunk sleeps overshoot deterministically; the real path pays none.)
     */
    public function testKeysDrainsChunkedRecordWithoutIdleSleepPerChunk(): void
    {
        // CONSUMER.CREATE reply (sid 1): one record pending, so keys() proceeds into the replay wait loop.
        $consumerReply = '{"stream_name":"KV_cfg","name":"KEYS","num_pending":1,'
            . '"config":{"deliver_subject":"_INBOX.KV.KEYS.x","ack_policy":"none",'
            . '"deliver_policy":"last_per_subject","headers_only":true}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($consumerReply), $consumerReply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // A single live headers-only delivery on the deliver subscription (sid 2). Its $JS.ACK last
        // token is num_pending=0, so completing this one frame signals "caught up". The X-Pad header
        // inflates the frame to ~360 bytes so, split one byte per chunk, a per-chunk 1 ms sleep would
        // accrue ~360 ms - far past the 0.2 s bound - while the real path pays nothing.
        $hdrs = "NATS/1.0\r\nNats-Sequence: 7\r\nX-Pad: " . str_repeat('a', 260) . "\r\n\r\n";
        $h = strlen($hdrs);
        $frame = sprintf("HMSG \$KV.cfg.email 2 \$JS.ACK.KV_cfg.KEYS.1.7.1.0.0 %d %d\r\n%s\r\n", $h, $h, $hdrs);

        $chunks = str_split($frame); // one byte per chunk: many partial-progress reads before completion
        self::assertGreaterThan(200, count($chunks), 'need enough chunks that a per-chunk sleep overshoots the bound');
        foreach ($chunks as $byte) {
            $transport->pushReadChunk($byte);
        }

        // 0.2 s progress bound: the real path drains all chunks well inside it; a mutant that sleeps 1 ms
        // per chunk cannot.
        $keys = $client->jetStream()->keyValue('cfg')->keys(0.2)->await();

        // REAL behavior: the chunked record is drained with no per-chunk idle sleep, so the live key is
        // collected and returned. On the mutant keys() throws before this point.
        self::assertSame(['email'], $keys);

        // Tear down and quiesce any residual TimeoutCancellation/delay timers the replay left pending.
        $client->disconnect()->await();
        foreach (EventLoop::getIdentifiers() as $id) {
            EventLoop::cancel($id);
        }
    }

    /**
     * kills UnwrapFinally @ line 711.
     *
     * history()'s replay wait loop is wrapped in `try { ... } finally { unsubscribe($sid) }`. When the
     * replay makes NO progress (no delivery ever arrives) the loop throws a stalled-history
     * JetStreamException bounded by the CALLER's progress timeout. The finally guarantees the deliver
     * subscription is still torn down on that throw path.
     *
     * MUTANT (finally unwrapped: the loop body runs bare and unsubscribe moved AFTER it): the throw now
     * escapes before unsubscribe runs, so NO `UNSUB 2` is written. Asserting the UNSUB is present on the
     * throw path kills it. The message assertions also pin the caller's 0.05 s bound and the stalled
     * banner/tail so the exception itself is the real one.
     */
    public function testHistoryThrowsOnStalledReplayAndStillUnsubscribes(): void
    {
        // CONSUMER.CREATE reply (sid 1): num_pending > 0 so history() proceeds to the replay, but no
        // deliver frame is ever seeded, so the consumer never reports "caught up" and the clock expires.
        $consumerReply = '{"stream_name":"KV_cfg","name":"HIST","num_pending":1,'
            . '"config":{"deliver_subject":"d","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($consumerReply), $consumerReply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        try {
            $client->jetStream()->keyValue('cfg')->history('theme', 0.05)->await();
            self::fail('expected a stalled-history JetStreamException');
        } catch (JetStreamException $e) {
            // Pin the message: opens with the stalled banner for this key, quotes the CALLER's 0.05 s
            // bound (not the 5 s default), and closes with the not-caught-up tail.
            self::assertStringStartsWith('Key history for "theme" stalled', $e->getMessage());
            self::assertStringContainsString('0.050 s', $e->getMessage());
            self::assertStringContainsString('replay not caught up', $e->getMessage());
        }

        // The deliver subscription (sid 2) is unsubscribed even on the throw path - guaranteed by the
        // finally the mutant removes.
        self::assertStringContainsString("UNSUB 2\r\n", implode('', $transport->writes));

        // Tear down and cancel any event-loop timers the timed-out replay may have left pending, so no
        // residual callback fires into a later test's clean loop.
        $client->disconnect()->await();
        foreach (EventLoop::getIdentifiers() as $id) {
            EventLoop::cancel($id);
        }
    }
}
