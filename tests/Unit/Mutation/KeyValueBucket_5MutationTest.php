<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\JetStream\KeyValue\KeyValueEntry;
use IDCT\NATS\JetStream\KeyValue\KeyWatchOptions;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

/**
 * Mutation-killing tests for {@see \IDCT\NATS\JetStream\KeyValue\KeyValueBucket} (chunk 5).
 *
 * Pins two survivors the earlier chunks left uncovered:
 *   - TrueValue @ 455  - the IN-HANDLER end-of-initial-data latch (re-fire suppression across
 *     multiple caught-up deliveries; chunk 2 only pinned the onConsumerCreated latch at 467).
 *   - CastInt  @ 882   - getStatus()'s last_sequence int-cast (chunk 2's fixture used an integer
 *     last_seq, so the cast was never exercised; a float last_seq forces it).
 */
final class KeyValueBucket_5MutationTest extends TestCase
{
    private const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    /**
     * watch() arms an idle-heartbeat watchdog (#113) - a live EventLoop repeat timer that would outlive
     * a test whose client is never disconnect()ed. Cancel every registered callback so a leaked timer
     * from one test cannot fire into another's clean loop.
     */
    protected function tearDown(): void
    {
        foreach (EventLoop::getIdentifiers() as $id) {
            EventLoop::cancel($id);
        }
    }

    private function connect(FakeTransport $transport): NatsClient
    {
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        return $client;
    }

    /**
     * Post-#118 mux request inbox: one long-lived wildcard "<base>.*" (sid 1) serves every reply and each
     * request routes by its "<base>.<token>" reply-to, so a pre-seeded fixed-subject frame is dropped.
     * This learns the mux base from the wildcard SUB and, for each request PUB/HPUB, echoes the next
     * queued frame FIFO on the CAPTURED reply-to with the mux sid.
     *
     * @param list<string> $frames Pre-built MSG/HMSG reply frames; only their subject+sid are rewritten.
     */
    private function muxReplies(FakeTransport $transport, array $frames): void
    {
        $queue = $frames;
        $base = null;

        $transport->onWrite = static function (string $bytes) use (&$queue, &$base): array {
            $head = strtok($bytes, "\r\n");
            if ($head === false) {
                return [];
            }

            if (str_starts_with($head, 'SUB ')) {
                $subject = explode(' ', $head)[1] ?? '';
                if (str_starts_with($subject, '_INBOX.') && str_ends_with($subject, '.*')) {
                    $base = substr($subject, 0, -1); // strip trailing "*" -> "_INBOX.<hex>."
                }

                return [];
            }

            if ($base === null || (!str_starts_with($head, 'PUB ') && !str_starts_with($head, 'HPUB '))) {
                return [];
            }

            $replyTo = explode(' ', $head)[2] ?? ''; // PUB <subj> <replyTo> <len> | HPUB <subj> <replyTo> ...
            if (!str_starts_with($replyTo, $base) || $queue === []) {
                return [];
            }

            return [self::rewriteReplyFrame(array_shift($queue), $replyTo)];
        };
    }

    /** Rewrites a reply frame's subject (token 1) + sid (token 2) to the captured mux reply-to / mux sid 1. */
    private static function rewriteReplyFrame(string $frame, string $replyTo): string
    {
        $pos = strpos($frame, "\r\n");
        self::assertNotFalse($pos, 'reply frame must have a head line');

        $tokens = explode(' ', substr($frame, 0, $pos));
        $tokens[1] = $replyTo;
        $tokens[2] = '1';

        return implode(' ', $tokens) . substr($frame, $pos);
    }

    /**
     * Mux-aware server for watch(): echoes $createReply on the CONSUMER.CREATE request's captured mux
     * reply-to, then emits each $deliveries frame on the ephemeral push deliver subscription using the
     * sid the server ACTUALLY assigned (captured live from its SUB), so a delivery cannot be consumed
     * before its subscription exists.
     *
     * @param list<string> $deliveries Pre-built MSG/HMSG deliver frames; only their sid is rewritten.
     */
    private function watchServer(FakeTransport $transport, string $createReply, array $deliveries = []): void
    {
        $base = null;
        $createSent = false;

        $transport->onWrite = static function (string $bytes) use (&$base, &$createSent, $createReply, $deliveries): array {
            $head = strtok($bytes, "\r\n");
            if ($head === false) {
                return [];
            }

            if (str_starts_with($head, 'SUB ')) {
                $parts = explode(' ', $head);
                $subject = $parts[1] ?? '';
                if (str_starts_with($subject, '_INBOX.') && str_ends_with($subject, '.*')) {
                    $base = substr($subject, 0, -1);

                    return [];
                }

                // The ephemeral push deliver subscription: emit the queued deliveries on its live sid.
                $deliverSid = (int) ($parts[2] ?? 0);

                return array_map(
                    static fn(string $d): string => self::rewriteFrameSid($d, $deliverSid),
                    $deliveries,
                );
            }

            if ($base === null || $createSent || (!str_starts_with($head, 'PUB ') && !str_starts_with($head, 'HPUB '))) {
                return [];
            }

            $replyTo = explode(' ', $head)[2] ?? '';
            if (!str_starts_with($replyTo, $base)) {
                return [];
            }
            $createSent = true;

            return [self::rewriteReplyFrame($createReply, $replyTo)];
        };
    }

    /** Rewrites only a delivery frame's sid (token 2), leaving its subject and $JS.ACK reply-to intact. */
    private static function rewriteFrameSid(string $frame, int $sid): string
    {
        $pos = strpos($frame, "\r\n");
        self::assertNotFalse($pos, 'delivery frame must have a head line');

        $tokens = explode(' ', substr($frame, 0, $pos));
        $tokens[2] = (string) $sid;

        return implode(' ', $tokens) . substr($frame, $pos);
    }

    // ─── watch() in-handler caught-up latch (line 455) ───────────────────────

    /**
     * kills TrueValue @ line 455 (`$caughtUpFired = true;` -> `= false;`) - the IN-HANDLER latch.
     *
     * The consumer is created with num_pending=1, so the onConsumerCreated immediate path does NOT fire;
     * the caught-up signal is driven purely from deliveries. TWO deliveries arrive, each reporting
     * num_pending=0 in its $JS.ACK reply.
     *
     * REAL: the first delivery fires onCaughtUp and latches $caughtUpFired=true; the second delivery sees
     * `!$caughtUpFired` == false and does NOT re-fire -> onCaughtUp fires exactly ONCE.
     *
     * MUTANT (`$caughtUpFired = false;`): the first delivery fires but leaves the latch false, so the
     * second (also num_pending=0) re-enters the block and fires AGAIN -> onCaughtUp fires TWICE. Asserting
     * the count is exactly 1 kills it. (Chunk 2 only pinned the SEPARATE onConsumerCreated latch at 467,
     * which this test's num_pending=1 deliberately bypasses, so the in-handler latch was never exercised.)
     */
    public function testWatchInHandlerCaughtUpLatchFiresOnlyOnceAcrossDeliveries(): void
    {
        $createReply = '{"stream_name":"KV_cfg","name":"KVWATCH","num_pending":1,'
            . '"config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none"}}';

        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $this->watchServer(
            $transport,
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
            [
                // Two PUT deliveries, each whose $JS.ACK last token (num_pending) is 0 -> both "caught
                // up". Consumer sequences 1 then 2: the ordered-consumer watch verifies contiguity,
                // and a repeated cseq would read as a gap and trigger a recreate instead of delivery.
                "MSG \$KV.cfg.theme 2 \$JS.ACK.KV_cfg.KVWATCH.1.7.1.0.0 4\r\nblue\r\n",
                "MSG \$KV.cfg.alpha 2 \$JS.ACK.KV_cfg.KVWATCH.2.8.2.0.0 5\r\ngreen\r\n",
            ],
        );

        $client = $this->connect($transport);

        $caughtUpCount = 0;
        $handled = 0;
        $client->jetStream()->keyValue('cfg')->watch(
            static function (KeyValueEntry $entry) use (&$handled): void {
                $handled++;
            },
            '>',
            new KeyWatchOptions(onCaughtUp: static function () use (&$caughtUpCount): void {
                $caughtUpCount++;
            }),
        )->await();

        // Immediate (onConsumerCreated) path must NOT fire: num_pending=1 at creation.
        self::assertSame(0, $caughtUpCount);

        // processIncoming() drains ONE chunk per call; pump enough times to deliver both frames (extra
        // calls are harmless no-ops - the FakeTransport read queue returns '' once exhausted).
        for ($i = 0; $i < 4; $i++) {
            $client->processIncoming()->await();
        }

        // Both deliveries were handled (the value callback ran twice) ...
        self::assertSame(2, $handled);
        // ... but the end-of-initial-data signal fired exactly once - the in-handler latch suppressed the
        // second num_pending=0 delivery. On the mutant it fires twice.
        self::assertSame(1, $caughtUpCount, 'onCaughtUp must fire once even across multiple caught-up deliveries');

        $client->disconnect()->await();
        foreach (EventLoop::getIdentifiers() as $id) {
            EventLoop::cancel($id);
        }
    }

    // ─── getStatus() last_sequence int-cast (line 882) ───────────────────────

    /**
     * kills CastInt @ line 882 (`(int) ($state['last_seq'] ?? ...)` -> cast removed).
     *
     * The stream state reports last_seq as a JSON float (12.0). REAL code (int)-casts it, so
     * getStatus()['last_sequence'] is the int 12. The MUTANT drops the cast and returns the float 12.0,
     * which fails the strict assertSame(12, ...) (int !== float). Chunk 2's fixture used an integer
     * last_seq (12), so removing the cast left 12 unchanged and the mutant survived - a float forces it.
     */
    public function testGetStatusCastsFloatLastSeqToInt(): void
    {
        // last_seq is a JSON float -> decodes to PHP float 12.0; messages/bytes stay ints so only the
        // last_sequence cast is under test here.
        $streamInfo = '{"config":{"name":"KV_cfg"},"state":{"messages":7,"last_seq":12.0,"bytes":64}}';

        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfo), $streamInfo),
        ]);

        $status = $this->connect($transport)->jetStream()->keyValue('cfg')->getStatus()->await();

        self::assertSame(12, $status['last_sequence']);
    }
}
