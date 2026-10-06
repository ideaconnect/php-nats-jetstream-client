<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\LoopTickCountingTransport;
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
     * false and NO 1 ms idle sleep is paid per chunk (#119) - the reads follow each other without the
     * event loop ticking between them, and keys() returns ['email'].
     *
     * MUTANT (`if ($read->consumedBytes)`): every partial-frame read now DOES sleep 1 ms, and the event
     * loop ticks during each sleep. The transport counts the chunks read after a tick
     * ({@see LoopTickCountingTransport}): none on the real code, every chunk of the record after the
     * first on the mutant, so the assertSame(0, ...) below fails, killing the mutant. The count does not
     * depend on how fast the machine is, and the 5 s progress bound gives the real code all the time it
     * needs to collect the key (#176).
     */
    public function testKeysDrainsChunkedRecordWithoutIdleSleepPerChunk(): void
    {
        // CONSUMER.CREATE reply: one record pending, so keys() proceeds into the replay wait loop. Post-
        // #118 the request inbox is MUXED (one long-lived wildcard "<base>.*" serves every reply), so a
        // fixed pre-seeded "MSG _INBOX.a 1" no longer matches the request's random "<base>.<token>" and
        // would be dropped. The onWrite responder below echoes this reply on the request's CAPTURED mux
        // reply-to (mux sid 1) instead.
        $consumerReply = '{"stream_name":"KV_cfg","name":"KEYS","num_pending":1,'
            . '"config":{"deliver_subject":"_INBOX.KV.KEYS.x","ack_policy":"none",'
            . '"deliver_policy":"last_per_subject","headers_only":true}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $counting = new LoopTickCountingTransport($transport);

        // Small request timeout so a mis-delivered mux reply fails fast instead of hanging the 10 s default.
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 2000), $counting);
        $client->connect()->await();

        // A single live headers-only delivery on the deliver subscription (sid 2). Its $JS.ACK last
        // token is num_pending=0, so completing this one frame signals "caught up". The X-Pad header
        // inflates the frame to ~360 bytes, so split one byte per chunk it takes ~360 reads, and a mutant
        // that sleeps 1 ms per partial read lets the event loop tick ~360 times.
        $hdrs = "NATS/1.0\r\nNats-Sequence: 7\r\nX-Pad: " . str_repeat('a', 260) . "\r\n\r\n";
        $h = strlen($hdrs);
        $frame = sprintf("HMSG \$KV.cfg.email 2 \$JS.ACK.KV_cfg.KEYS.1.7.1.0.0 %d %d\r\n%s\r\n", $h, $h, $hdrs);

        $chunks = str_split($frame); // one byte per chunk: many partial-progress reads before completion
        self::assertGreaterThan(200, count($chunks), 'the record must span many one-byte chunks');

        // Mux request inbox (#118): echo the CONSUMER.CREATE reply on the request's captured reply-to
        // (mux sid, learned from the wildcard SUB), then release the chunked deliver frame - one transport
        // byte per read - ONLY once the deliver subscription is written. Emitting the chunks on the deliver
        // SUB (rather than pre-seeding them) keeps them from being consumed and dropped while the
        // CONSUMER.CREATE request is still in flight (its reply would otherwise land behind them). The count
        // of reads after a loop tick starts there too, so it covers the record's chunks only.
        $muxSid = 1;
        $transport->onWrite = static function (string $bytes) use ($consumerReply, $chunks, &$muxSid, $counting): array {
            $head = strtok($bytes, "\r\n");
            if ($head === false) {
                return [];
            }

            $parts = explode(' ', $head);
            if (str_starts_with($head, 'SUB ')) {
                $subject = $parts[1] ?? '';
                if (str_starts_with($subject, '_INBOX.') && str_ends_with($subject, '.*')) {
                    $muxSid = (int) ($parts[2] ?? 1); // learn the mux inbox sid from its wildcard SUB
                } elseif (str_starts_with($subject, '_INBOX.KV.KEYS')) {
                    $counting->reset();

                    return $chunks; // deliver subscription live: release the record one byte per read
                }

                return [];
            }

            if (str_starts_with($head, 'PUB ') && str_contains($head, '$JS.API.CONSUMER.CREATE.')) {
                $replyTo = $parts[2] ?? ''; // PUB <subject> <replyTo> <len>
                if ($replyTo !== '') {
                    return [sprintf("MSG %s %d %d\r\n%s\r\n", $replyTo, $muxSid, strlen($consumerReply), $consumerReply)];
                }
            }

            return [];
        };

        // A 5 s progress bound: the real path drains all chunks long before it, however slow the machine.
        $keys = $client->jetStream()->keyValue('cfg')->keys(5.0)->await();

        // The chunked record is drained and its live key collected and returned.
        self::assertSame(['email'], $keys);
        self::assertSame(count($chunks), $counting->chunksRead, 'every byte of the record must be read as its own chunk');
        // REAL behavior: no per-chunk idle sleep, so the event loop never ticks between two of the record's
        // reads. The mutant sleeps 1 ms after each partial read and the loop ticks every time.
        self::assertSame(
            0,
            $counting->chunksReadAfterALoopTick,
            'keys() must read the next chunk at once, not idle-sleep 1 ms per partial chunk',
        );

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
        // CONSUMER.CREATE reply: num_pending > 0 so history() proceeds to the replay, but no deliver frame
        // is ever seeded, so the consumer never reports "caught up" and the clock expires. Post-#118 the
        // request inbox is MUXED, so this reply is echoed on the request's captured mux reply-to (mux sid 1)
        // by the onWrite responder below rather than pre-seeded on a fixed "_INBOX.a" subject.
        $consumerReply = '{"stream_name":"KV_cfg","name":"HIST","num_pending":1,'
            . '"config":{"deliver_subject":"d","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        // Small request timeout so a mis-delivered mux reply fails fast instead of hanging the 10 s default.
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 2000), $transport);
        $client->connect()->await();

        // Mux request inbox (#118): the deliver subscription (sid 2) intentionally gets NO frame - the
        // replay makes no progress and stalls - so this responder ONLY echoes the CONSUMER.CREATE reply on
        // the request's captured reply-to (mux sid, learned from the wildcard SUB).
        $muxSid = 1;
        $transport->onWrite = static function (string $bytes) use ($consumerReply, &$muxSid): array {
            $head = strtok($bytes, "\r\n");
            if ($head === false) {
                return [];
            }

            $parts = explode(' ', $head);
            if (str_starts_with($head, 'SUB ')) {
                $subject = $parts[1] ?? '';
                if (str_starts_with($subject, '_INBOX.') && str_ends_with($subject, '.*')) {
                    $muxSid = (int) ($parts[2] ?? 1); // learn the mux inbox sid from its wildcard SUB
                }

                return [];
            }

            if (str_starts_with($head, 'PUB ') && str_contains($head, '$JS.API.CONSUMER.CREATE.')) {
                $replyTo = $parts[2] ?? ''; // PUB <subject> <replyTo> <len>
                if ($replyTo !== '') {
                    return [sprintf("MSG %s %d %d\r\n%s\r\n", $replyTo, $muxSid, strlen($consumerReply), $consumerReply)];
                }
            }

            return [];
        };

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
