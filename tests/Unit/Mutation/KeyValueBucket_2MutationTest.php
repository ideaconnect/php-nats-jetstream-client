<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\KeyValue\KeyValueEntry;
use IDCT\NATS\JetStream\KeyValue\KeyWatchOptions;
use IDCT\NATS\Tests\Support\FakeTransport;
use Revolt\EventLoop;

/**
 * Mutation-killing tests for {@see \IDCT\NATS\JetStream\KeyValue\KeyValueBucket} (chunk 2).
 *
 * Each method pins the exact observable behavior that a surviving mutant would change. The mutants
 * cluster around createKey() error/revision handling, history() pagination guards + cleanup,
 * getStatus() casting/coalescing, and requestStreamInfoWithSubjects() request shaping.
 */
final class KeyValueBucket_2MutationTest extends \PHPUnit\Framework\TestCase
{
    // Pre-2.11 server: getAll() takes the per-subject Direct Get fan-out (the code these mutants pin);
    // batched multi_last Direct Get requires 2.11+ and is covered separately by the #110 pins.
    private const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.10.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    /** Direct Get reply (HMSG): stored value as body with Nats-* (+ optional KV-Operation). */
    private function kvDirectReply(string $subject, string $value, ?int $seq, int $sid, ?string $operation = null): string
    {
        $hdrs = "NATS/1.0\r\nNats-Stream: KV_cfg\r\nNats-Subject: {$subject}\r\n";
        if ($seq !== null) {
            $hdrs .= "Nats-Sequence: {$seq}\r\n";
        }
        if ($operation !== null) {
            $hdrs .= "KV-Operation: {$operation}\r\n";
        }
        $hdrs .= "\r\n";
        $h = strlen($hdrs);

        return sprintf("HMSG _INBOX.x %d %d %d\r\n%s%s\r\n", $sid, $h, $h + strlen($value), $hdrs, $value);
    }

    private function kvDirectStatus(int $sid, int $code, string $description): string
    {
        $hdrs = "NATS/1.0 {$code} {$description}\r\nStatus: {$code}\r\n\r\n";
        $h = strlen($hdrs);

        return sprintf("HMSG _INBOX.x %d %d %d\r\n%s\r\n", $sid, $h, $h, $hdrs);
    }

    /** Returns the last HPUB frame written to the transport (the createKey recreate publish). */
    private function lastHpub(FakeTransport $transport): string
    {
        $hpub = '';
        foreach ($transport->writes as $w) {
            if (str_starts_with($w, 'HPUB ')) {
                $hpub = $w;
            }
        }
        self::assertNotSame('', $hpub, 'Expected an HPUB frame to be written.');

        return $hpub;
    }

    /** @param list<string> $queue */
    private function connect(array $queue): NatsClient
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport(array_merge([self::INFO, "PONG\r\n"], $queue)));
        $client->connect()->await();

        return $client;
    }

    /**
     * A watch()'s idle-heartbeat watchdog (#113) is a live EventLoop repeat timer that outlives a test
     * whose client is never disconnect()ed. Cancel every callback left registered so a leaked watchdog
     * from one test cannot fire against a lingering client during another.
     */
    protected function tearDown(): void
    {
        foreach (EventLoop::getIdentifiers() as $id) {
            EventLoop::cancel($id);
        }
    }

    /**
     * Post-#118 the request inbox is MUXED: request()/requestWithHeaders() no longer subscribe a fresh
     * inbox per call; instead ONE long-lived wildcard "<base>.*" (sid 1, established on the FIRST request
     * before any deliver subscription in every test here) serves every reply, and each request publishes
     * reply-to "<base>.<token>" and routes by that token. A pre-seeded "MSG _INBOX.x <sid>" therefore no
     * longer reaches the waiting request (its subject is not the request's random "<base>.<token>", so the
     * reply is dropped and the request times out). This builds a client whose FakeTransport mirrors a real
     * server: it learns the mux base from the wildcard SUB and, for each request PUB/HPUB, pops the next
     * reply frame and re-emits it FIFO on the CAPTURED reply-to with the mux sid (1). Subscription
     * deliveries (a push consumer's deliver subject) are NOT mux replies - enqueue those on their SUB via
     * $transport->enqueueOnWriteContaining so they arrive after the deliver subscription exists.
     *
     * @param list<string> $muxFrames Pre-built MSG/HMSG reply frames; only their subject+sid are rewritten.
     * @return array{NatsClient, FakeTransport}
     */
    private function connectMux(array $muxFrames): array
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $this->muxReplies($transport, $muxFrames);
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        return [$client, $transport];
    }

    /** @param list<string> $frames */
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
                // Learn the mux base from the wildcard registration "SUB <base>.* <sid>".
                $subject = explode(' ', $head)[1] ?? '';
                if (str_starts_with($subject, '_INBOX.') && str_ends_with($subject, '.*')) {
                    $base = substr($subject, 0, -1); // strip the trailing "*" -> "_INBOX.<hex>."
                }

                return [];
            }

            if ($base === null || (!str_starts_with($head, 'PUB ') && !str_starts_with($head, 'HPUB '))) {
                return [];
            }

            // PUB <subject> <replyTo> <len>  |  HPUB <subject> <replyTo> <hdrLen> <totLen>
            $replyTo = explode(' ', $head)[2] ?? '';
            if (!str_starts_with($replyTo, $base) || $queue === []) {
                // A plain publish or a subscription's own inbox - not a mux request.
                return [];
            }

            return [self::rewriteReplyFrame(array_shift($queue), $replyTo)];
        };
    }

    /**
     * Rewrites a pre-built MSG/HMSG reply frame's subject and sid to the request's captured mux reply-to
     * and the mux sid (1), preserving the payload/header bytes and declared lengths verbatim so the
     * reply's content is delivered unchanged - only its addressing is corrected for the mux inbox.
     */
    private static function rewriteReplyFrame(string $frame, string $replyTo): string
    {
        $pos = strpos($frame, "\r\n");
        self::assertNotFalse($pos, 'reply frame must have a head line');

        $tokens = explode(' ', substr($frame, 0, $pos));
        $tokens[1] = $replyTo; // subject
        $tokens[2] = '1';      // mux sid

        return implode(' ', $tokens) . substr($frame, $pos);
    }

    // ─── createKey(): validation, "exists" error, recreate revision ─────────────────────────────

    public function testCreateKeyValidatesKeyBeforePublishing(): void
    {
        // kills MethodCallRemoval @ 450 - removing assertValidKey() would let an invalid key through.
        $kv = $this->connect([])->jetStream()->keyValue('cfg');

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid KV key');
        $kv->createKey('a b', 'v')->await();
    }

    public function testCreateKeyThrowsExactKeyExistsMessageAndCode(): void
    {
        // First exclusive create (expected seq 0) is rejected wrong-last-sequence; get() then shows a
        // live value, so the key really exists.
        $errAck = '{"error":{"code":400,"err_code":10071,"description":"wrong last sequence: 4"}}';
        [$client] = $this->connectMux([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errAck), $errAck),
            $this->kvDirectReply('$KV.cfg.theme', 'green', 4, 2),
        ]);
        $kv = $client->jetStream()->keyValue('cfg');

        try {
            $kv->createKey('theme', 'blue')->await();
            self::fail('Expected JetStreamException for an existing key.');
        } catch (JetStreamException $e) {
            // kills Concat + ConcatOperandRemoval @ 465 - message is exactly "<prefix><key>", not reordered/truncated.
            self::assertSame('Key already exists: theme', $e->getMessage());
            // kills IncrementInteger + DecrementInteger @ 465 - the codes are exactly 400 (HTTP-like)
            // and 10071 (API err_code, #154), not off by one.
            self::assertSame(400, $e->getCode());
            self::assertSame(10071, $e->getErrCode());
        }
    }

    public function testCreateKeyRecreatesAgainstTombstoneRevision(): void
    {
        // The latest record is a DEL tombstone at revision 4; recreate must assert that revision.
        $errAck = '{"error":{"code":400,"err_code":10071,"description":"wrong last sequence: 4"}}';
        $putAck = '{"stream":"KV_cfg","seq":5,"duplicate":false}';
        [$client, $transport] = $this->connectMux([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errAck), $errAck),
            $this->kvDirectReply('$KV.cfg.theme', 'ignored', 4, 2, 'DEL'),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($putAck), $putAck),
        ]);

        $ack = $client->jetStream()->keyValue('cfg')->createKey('theme', 'blue')->await();

        self::assertSame(5, $ack->seq);
        // The recreate HPUB (the second, post-tombstone publish) must carry the tombstone's revision (4).
        $recreate = $this->lastHpub($transport);
        self::assertStringContainsString('HPUB $KV.cfg.theme', $recreate);
        self::assertStringContainsString('Nats-Expected-Last-Subject-Sequence:4', $recreate);
    }

    public function testCreateKeyRecreatesWithZeroSeqWhenTombstoneHasNoRevision(): void
    {
        // kills IncrementInteger + DecrementInteger @ 468 (the `$entry->revision ?? 0` branch):
        // a DEL tombstone WITHOUT a Nats-Sequence header => revision null => expected seq must be 0.
        $errAck = '{"error":{"code":400,"err_code":10071,"description":"wrong last sequence"}}';
        $putAck = '{"stream":"KV_cfg","seq":9,"duplicate":false}';
        // A DEL tombstone with NO Nats-Sequence header => revision resolves to null -> ?? 0.
        [$client, $transport] = $this->connectMux([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errAck), $errAck),
            $this->kvDirectReply('$KV.cfg.theme', '', null, 2, 'DEL'),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($putAck), $putAck),
        ]);

        $ack = $client->jetStream()->keyValue('cfg')->createKey('theme', 'blue')->await();

        self::assertSame(9, $ack->seq);
        $recreate = $this->lastHpub($transport);
        self::assertStringContainsString('Nats-Expected-Last-Subject-Sequence:0', $recreate);
    }

    public function testCreateKeyRecreatesWithZeroSeqWhenEntryAbsent(): void
    {
        // kills IncrementInteger + DecrementInteger @ 468 (the `: 0` branch):
        // first create fails wrong-last-seq, then get() races to a 404 (entry null) => expected seq 0.
        $errAck = '{"error":{"code":400,"err_code":10071,"description":"wrong last sequence"}}';
        $putAck = '{"stream":"KV_cfg","seq":11,"duplicate":false}';
        [$client, $transport] = $this->connectMux([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errAck), $errAck),
            $this->kvDirectStatus(2, 404, 'Message Not Found'),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($putAck), $putAck),
        ]);

        $ack = $client->jetStream()->keyValue('cfg')->createKey('theme', 'blue')->await();

        self::assertSame(11, $ack->seq);
        $recreate = $this->lastHpub($transport);
        self::assertStringContainsString('Nats-Expected-Last-Subject-Sequence:0', $recreate);
    }

    // ─── history(): validation, consumer config, pending guard, cleanup ──────────────────────────

    public function testHistoryValidatesKeyBeforeAnyRequest(): void
    {
        // kills MethodCallRemoval @ 506.
        $kv = $this->connect([])->jetStream()->keyValue('cfg');

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid KV key');
        $kv->history('a b')->await();
    }

    public function testHistoryRequestsDeliverPolicyAll(): void
    {
        // kills ArrayItemRemoval @ 513 - dropping 'deliver_policy' => 'all' changes the consumer config.
        $createReply = '{"stream_name":"KV_cfg","name":"HIST","num_pending":0,"config":{"deliver_subject":"d","ack_policy":"none"}}';
        [$client, $transport] = $this->connectMux([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $client->jetStream()->keyValue('cfg')->history('theme')->await();

        $create = implode('', $transport->writes);
        self::assertStringContainsString('$JS.API.CONSUMER.CREATE.KV_cfg', $create);
        self::assertStringContainsString('"deliver_policy":"all"', $create);
    }

    public function testHistoryReturnsEmptyWhenPendingZeroEvenIfDeliveryQueued(): void
    {
        // num_pending=0 => history returns [] immediately. A delivery is ALSO queued, so any mutant
        // that skips/changes the guard would subscribe, process it, and return a 1-entry list.
        // kills DecrementInteger @ 517 ($pending === -1), ReturnRemoval @ 518 (return []).
        $createReply = '{"stream_name":"KV_cfg","name":"HIST","num_pending":0,"config":{"deliver_subject":"d","ack_policy":"none"}}';
        [$client, $transport] = $this->connectMux([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);
        // The delivery is NOT a mux reply: it can only be seen by a mutant that skips the guard and
        // subscribes to the deliver subject (sid 2). Enqueue it on that SUB so it lands only if such a
        // subscription is ever written - real code returns [] first and never subscribes.
        $transport->enqueueOnWriteContaining['SUB _INBOX.KV.HIST'] = [
            "MSG dlv 2 \$JS.ACK.KV_cfg.HIST.1.6.1.0.0 2\r\nv1\r\n",
        ];
        $kv = $client->jetStream()->keyValue('cfg');

        self::assertSame([], $kv->history('theme')->await());
    }

    public function testHistoryTreatsMissingPendingAsZero(): void
    {
        // kills IncrementInteger + DecrementInteger @ 516 (`num_pending ?? 0`): absent num_pending must
        // default to 0 -> []. A mutant default of 1/-1 would proceed and collect the queued delivery.
        $createReply = '{"stream_name":"KV_cfg","name":"HIST","config":{"deliver_subject":"d","ack_policy":"none"}}';
        [$client, $transport] = $this->connectMux([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);
        $transport->enqueueOnWriteContaining['SUB _INBOX.KV.HIST'] = [
            "MSG dlv 2 \$JS.ACK.KV_cfg.HIST.1.6.1.0.0 2\r\nv1\r\n",
        ];
        $kv = $client->jetStream()->keyValue('cfg');

        self::assertSame([], $kv->history('theme')->await());
    }

    public function testHistoryCastsPendingToInt(): void
    {
        // kills CastInt @ 516: a float 0.0 must (int)-cast to 0 (=== 0 true). Without the cast, 0.0 === 0
        // is false in PHP, so the mutant would proceed and collect the queued delivery.
        $createReply = '{"stream_name":"KV_cfg","name":"HIST","num_pending":0.0,"config":{"deliver_subject":"d","ack_policy":"none"}}';
        [$client, $transport] = $this->connectMux([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);
        $transport->enqueueOnWriteContaining['SUB _INBOX.KV.HIST'] = [
            "MSG dlv 2 \$JS.ACK.KV_cfg.HIST.1.6.1.0.0 2\r\nv1\r\n",
        ];
        $kv = $client->jetStream()->keyValue('cfg');

        self::assertSame([], $kv->history('theme')->await());
    }

    public function testHistoryUnsubscribesAfterCollecting(): void
    {
        // kills MethodCallRemoval @ 553 - the finally must unsubscribe the history delivery sid (2).
        $createReply = '{"stream_name":"KV_cfg","name":"HIST","num_pending":1,"config":{"deliver_subject":"d","ack_policy":"none"}}';
        [$client, $transport] = $this->connectMux([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);
        // The single replayed revision is a subscription delivery (sid 2 = the deliver sub; the mux inbox
        // is sid 1). Enqueue it on the deliver SUB so it arrives after that subscription exists.
        $transport->enqueueOnWriteContaining['SUB _INBOX.KV.HIST'] = [
            "MSG dlv 2 \$JS.ACK.KV_cfg.HIST.1.6.1.0.0 2\r\nv1\r\n",
        ];

        $history = $client->jetStream()->keyValue('cfg')->history('theme')->await();

        self::assertCount(1, $history);
        // The delivery subscription (sid 2) must be torn down once the replay completes.
        self::assertContains("UNSUB 2\r\n", $transport->writes);
    }

    // ─── getAll(): array slicing, request shaping ───────────────────────────────────────────────

    public function testGetAllReturnsEveryLiveKeyNotJustTheFirst(): void
    {
        // kills ArrayOneItem @ 620 - returning array_slice(..,0,1) would drop the second key.
        $page1 = '{"config":{"name":"KV_cfg","subjects":["$KV.cfg.>"]},"state":{"subjects":{"$KV.cfg.username":2,"$KV.cfg.email":2}}}';
        $page2 = '{"config":{"name":"KV_cfg"},"state":{"subjects":{}}}';
        $uHdrs = "NATS/1.0\r\nNats-Subject: \$KV.cfg.username\r\nNats-Sequence: 3\r\n\r\n";
        $eHdrs = "NATS/1.0\r\nNats-Subject: \$KV.cfg.email\r\nNats-Sequence: 4\r\n\r\n";
        $uv = 'alice';
        $ev = 'bob';
        $uh = strlen($uHdrs);
        $eh = strlen($eHdrs);
        // FIFO mux replies: the two STREAM.INFO pages are requested first (offset 0 then 2), then the
        // per-key Direct Gets are written in subject order (username, then email), so each reply lands
        // on the matching request's captured reply-to.
        [$client] = $this->connectMux([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($page1), $page1),
            sprintf("MSG _INBOX.p 2 %d\r\n%s\r\n", strlen($page2), $page2),
            sprintf("HMSG _INBOX.b 3 %d %d\r\n%s%s\r\n", $uh, $uh + strlen($uv), $uHdrs, $uv),
            sprintf("HMSG _INBOX.c 4 %d %d\r\n%s%s\r\n", $eh, $eh + strlen($ev), $eHdrs, $ev),
        ]);
        $kv = $client->jetStream()->keyValue('cfg');

        $all = $kv->getAll()->await();

        self::assertCount(2, $all);
        self::assertSame('alice', $all['username']);
        self::assertSame('bob', $all['email']);
    }

    public function testStreamInfoRequestSubjectAndPayloadAreExact(): void
    {
        $page = '{"config":{"name":"KV_cfg"},"state":{"subjects":{}}}';
        [$client, $transport] = $this->connectMux([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($page), $page),
        ]);

        $client->jetStream()->keyValue('cfg')->getAll()->await();

        $infoRequest = '';
        foreach ($transport->writes as $w) {
            if (str_contains($w, 'subjects_filter')) {
                $infoRequest = $w;
                break;
            }
        }
        self::assertNotSame('', $infoRequest, 'Expected a STREAM.INFO request to be written.');
        // kills ConcatOperandRemoval @ 678 - the request subject must keep the STREAM.INFO prefix.
        self::assertStringContainsString('PUB $JS.API.STREAM.INFO.KV_cfg ', $infoRequest);
        // kills ArrayItemRemoval @ 687 + Concat/ConcatOperandRemoval @ 688 - exact subjects_filter token.
        self::assertStringContainsString('"subjects_filter":"$KV.cfg.>"', $infoRequest);
        // kills DecrementInteger @ 684 - the first page must request offset 0.
        self::assertStringContainsString('"offset":0', $infoRequest);
    }

    // ─── getStatus(): casting + coalescing of stream-state counters ─────────────────────────────

    public function testGetStatusCastsCountersToIntAndPrefersLastSeq(): void
    {
        // Floats exercise the (int) casts; last_seq (12) differs from messages (7) to pin coalescing.
        $streamInfo = '{"config":{"name":"KV_cfg"},"state":{"messages":7.0,"last_seq":12,"bytes":128.0}}';
        [$client] = $this->connectMux([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfo), $streamInfo),
        ]);
        $status = $client->jetStream()->keyValue('cfg')->getStatus()->await();

        // kills CastInt @ 639/640/641 - strict identity fails against float 7.0/12.0/128.0.
        self::assertSame(7, $status['messages']);
        // kills Coalesce @ 640 - real prefers last_seq (12); the swapped mutant would yield messages (7).
        self::assertSame(12, $status['last_sequence']);
        self::assertSame(128, $status['bytes']);
    }

    public function testGetStatusDefaultsMissingCountersToZero(): void
    {
        // State carries none of messages/last_seq/bytes => every counter must default to 0.
        $streamInfo = '{"config":{"name":"KV_cfg"},"state":{}}';
        [$client] = $this->connectMux([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfo), $streamInfo),
        ]);
        $status = $client->jetStream()->keyValue('cfg')->getStatus()->await();

        // kills IncrementInteger + DecrementInteger @ 639 (messages ?? 0).
        self::assertSame(0, $status['messages']);
        // kills IncrementInteger + DecrementInteger @ 640 (nested messages ?? 0 when last_seq absent).
        self::assertSame(0, $status['last_sequence']);
        // kills IncrementInteger + DecrementInteger @ 641 (bytes ?? 0).
        self::assertSame(0, $status['bytes']);
    }

    // ─── watch(): end-of-initial-data fires exactly once ────────────────────────────────────────

    public function testWatchCaughtUpFiresOnceForEmptyBucketThenDelivery(): void
    {
        // Empty bucket (num_pending=0) fires onCaughtUp in onConsumerCreated and latches caughtUpFired.
        // A subsequent delivery (numPending=0) must NOT re-fire it.
        // kills TrueValue @ 430 - flipping the latch to false would let the in-handler check re-fire.
        $createReply = '{"stream_name":"KV_cfg","name":"KVWATCH","num_pending":0,"config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none"}}';
        [$client, $transport] = $this->connectMux([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);
        // The later delivery is a push-consumer delivery (sid 2 = the deliver sub; the mux inbox is sid 1),
        // not a mux reply. Enqueue it on the deliver SUB so it exists only once the watch subscribes, then
        // pump it explicitly below - it must NOT re-fire onCaughtUp (the latch is already set).
        $transport->enqueueOnWriteContaining['SUB _INBOX.JS.PUSH'] = [
            "MSG \$KV.cfg.theme 2 \$JS.ACK.KV_cfg.KVWATCH.1.7.1.0.0 4\r\nblue\r\n",
        ];

        $caught = 0;
        $options = new KeyWatchOptions(includeHistory: true, onCaughtUp: function () use (&$caught): void {
            $caught++;
        });
        $client->jetStream()->keyValue('cfg')->watch(static function (KeyValueEntry $entry): void {}, '>', $options)->await();

        self::assertSame(1, $caught, 'onCaughtUp must fire once during consumer creation for an empty bucket.');

        $client->processIncoming()->await(); // pump the queued delivery

        self::assertSame(1, $caught, 'onCaughtUp must not re-fire on a later delivery once already caught up.');
    }
}
