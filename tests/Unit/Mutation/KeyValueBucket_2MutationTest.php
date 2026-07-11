<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\KeyValue\KeyValueEntry;
use IDCT\NATS\JetStream\KeyValue\KeyWatchOptions;
use IDCT\NATS\Tests\Support\FakeTransport;

/**
 * Mutation-killing tests for {@see \IDCT\NATS\JetStream\KeyValue\KeyValueBucket} (chunk 2).
 *
 * Each method pins the exact observable behavior that a surviving mutant would change. The mutants
 * cluster around createKey() error/revision handling, history() pagination guards + cleanup,
 * getStatus() casting/coalescing, and requestStreamInfoWithSubjects() request shaping.
 */
final class KeyValueBucket_2MutationTest extends \PHPUnit\Framework\TestCase
{
    private const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

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
        $kv = $this->connect([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errAck), $errAck),
            $this->kvDirectReply('$KV.cfg.theme', 'green', 4, 2),
        ])->jetStream()->keyValue('cfg');

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
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errAck), $errAck),
            $this->kvDirectReply('$KV.cfg.theme', 'ignored', 4, 2, 'DEL'),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($putAck), $putAck),
        ]);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

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
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errAck), $errAck),
            $this->kvDirectReply('$KV.cfg.theme', '', null, 2, 'DEL'),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($putAck), $putAck),
        ]);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

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
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errAck), $errAck),
            $this->kvDirectStatus(2, 404, 'Message Not Found'),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($putAck), $putAck),
        ]);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

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
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

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
        $kv = $this->connect([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
            "MSG dlv 2 \$JS.ACK.KV_cfg.HIST.1.6.1.0.0 2\r\nv1\r\n",
        ])->jetStream()->keyValue('cfg');

        self::assertSame([], $kv->history('theme')->await());
    }

    public function testHistoryTreatsMissingPendingAsZero(): void
    {
        // kills IncrementInteger + DecrementInteger @ 516 (`num_pending ?? 0`): absent num_pending must
        // default to 0 -> []. A mutant default of 1/-1 would proceed and collect the queued delivery.
        $createReply = '{"stream_name":"KV_cfg","name":"HIST","config":{"deliver_subject":"d","ack_policy":"none"}}';
        $kv = $this->connect([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
            "MSG dlv 2 \$JS.ACK.KV_cfg.HIST.1.6.1.0.0 2\r\nv1\r\n",
        ])->jetStream()->keyValue('cfg');

        self::assertSame([], $kv->history('theme')->await());
    }

    public function testHistoryCastsPendingToInt(): void
    {
        // kills CastInt @ 516: a float 0.0 must (int)-cast to 0 (=== 0 true). Without the cast, 0.0 === 0
        // is false in PHP, so the mutant would proceed and collect the queued delivery.
        $createReply = '{"stream_name":"KV_cfg","name":"HIST","num_pending":0.0,"config":{"deliver_subject":"d","ack_policy":"none"}}';
        $kv = $this->connect([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
            "MSG dlv 2 \$JS.ACK.KV_cfg.HIST.1.6.1.0.0 2\r\nv1\r\n",
        ])->jetStream()->keyValue('cfg');

        self::assertSame([], $kv->history('theme')->await());
    }

    public function testHistoryUnsubscribesAfterCollecting(): void
    {
        // kills MethodCallRemoval @ 553 - the finally must unsubscribe the history delivery sid (2).
        $createReply = '{"stream_name":"KV_cfg","name":"HIST","num_pending":1,"config":{"deliver_subject":"d","ack_policy":"none"}}';
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
            "MSG dlv 2 \$JS.ACK.KV_cfg.HIST.1.6.1.0.0 2\r\nv1\r\n",
        ]);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

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
        $kv = $this->connect([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($page1), $page1),
            sprintf("MSG _INBOX.p 2 %d\r\n%s\r\n", strlen($page2), $page2),
            sprintf("HMSG _INBOX.b 3 %d %d\r\n%s%s\r\n", $uh, $uh + strlen($uv), $uHdrs, $uv),
            sprintf("HMSG _INBOX.c 4 %d %d\r\n%s%s\r\n", $eh, $eh + strlen($ev), $eHdrs, $ev),
        ])->jetStream()->keyValue('cfg');

        $all = $kv->getAll()->await();

        self::assertCount(2, $all);
        self::assertSame('alice', $all['username']);
        self::assertSame('bob', $all['email']);
    }

    public function testStreamInfoRequestSubjectAndPayloadAreExact(): void
    {
        $page = '{"config":{"name":"KV_cfg"},"state":{"subjects":{}}}';
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($page), $page),
        ]);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

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
        $status = $this->connect([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfo), $streamInfo),
        ])->jetStream()->keyValue('cfg')->getStatus()->await();

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
        $status = $this->connect([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfo), $streamInfo),
        ])->jetStream()->keyValue('cfg')->getStatus()->await();

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
        $client = $this->connect([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
            "MSG \$KV.cfg.theme 2 \$JS.ACK.KV_cfg.KVWATCH.1.7.1.0.0 4\r\nblue\r\n",
        ]);

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
