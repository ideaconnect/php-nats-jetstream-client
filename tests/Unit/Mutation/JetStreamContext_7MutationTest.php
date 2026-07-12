<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

/**
 * Mutation-killing tests for the #119 read-path change in src/JetStream/JetStreamContext.php
 * (directGetBatch() and fetchBatch()).
 *
 * Two mutant families are pinned here:
 *  - UnwrapFinally on the try/finally that guarantees the inbox subscription is UNSUBbed even when
 *    the collect loop throws (a stall / heartbeat-miss). The mutant hoists the unsubscribe out of the
 *    finally, so a throw skips it: we assert an UNSUB frame reaches the wire despite the exception.
 *  - LogicalNot on the `if (!$read->consumedBytes)` idle-sleep guard. The real code sleeps 1 ms ONLY
 *    on a genuinely idle read and loops immediately on a byte-consuming (partial-frame) read; the
 *    mutant inverts that, paying a 1 ms sleep per chunk of a multi-chunk payload. We feed a payload
 *    split into >1001 one-byte chunks against a ~1001 ms internal deadline (expiresMs=1): the real
 *    path drains it in well under the deadline and returns the message, whereas a 1 ms-per-chunk
 *    mutant blows the deadline and throws instead of returning the message.
 *
 * All frames are driven through the in-process FakeTransport (no sockets, no Docker); the collect
 * loop itself pumps the reads.
 */
final class JetStreamContext_7MutationTest extends TestCase
{
    private const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    private function connect(FakeTransport $transport): NatsClient
    {
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        return $client;
    }

    // kills UnwrapFinally @ 857 (directGetBatch)
    //
    // A silent server (blockWhenEmpty, no batch frames, expiresMs=1) makes the collect loop stall and
    // throw after ~1001 ms. The real code UNSUBs the private inbox from the finally regardless; the
    // UnwrapFinally mutant hoists that unsubscribe past the loop so the stall throw skips it. Assert the
    // UNSUB reached the wire despite the exception.
    public function testDirectGetBatchUnsubscribesInboxEvenWhenBatchStalls(): void
    {
        $transport = new FakeTransport(
            readQueue: [self::INFO, "PONG\r\n"],
            blockWhenEmpty: true,
        );
        $client = $this->connect($transport);

        $threw = false;
        try {
            $client->jetStream()->directGetBatch('ORDERS', ['batch' => 10], 1)->await();
        } catch (JetStreamException $e) {
            $threw = true;
            self::assertStringContainsString('stalled', $e->getMessage());
        }

        self::assertTrue($threw, 'a stalled batch must surface as a JetStreamException');

        $unsub = array_filter($transport->writes, static fn (string $w): bool => str_contains($w, 'UNSUB'));
        self::assertNotSame(
            [],
            $unsub,
            'the private inbox must be UNSUBbed from the finally even when the batch stalls and throws',
        );
    }

    // kills UnwrapFinally @ 2058 (fetchBatch)
    //
    // With idle_heartbeat requested and a silent server, fetchBatch throws a heartbeat-miss after ~2
    // intervals (~100 ms). The real code UNSUBs the inbox from the finally; the UnwrapFinally mutant
    // hoists the unsubscribe past the loop so the throw skips it. Assert the UNSUB reached the wire.
    public function testFetchBatchUnsubscribesInboxEvenWhenHeartbeatMissThrows(): void
    {
        $transport = new FakeTransport(
            readQueue: [self::INFO, "PONG\r\n"],
            blockWhenEmpty: true,
        );
        $client = $this->connect($transport);

        $threw = false;
        try {
            // 50 ms heartbeat vs 2000 ms expiry: two silent intervals (~100 ms) trip the miss.
            $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2000, ['idle_heartbeat' => 50_000_000])->await();
        } catch (JetStreamException $e) {
            $threw = true;
            self::assertStringContainsString('missed idle heartbeats', $e->getMessage());
        }

        self::assertTrue($threw, 'a missed heartbeat must surface as a JetStreamException');

        $unsub = array_filter($transport->writes, static fn (string $w): bool => str_contains($w, 'UNSUB'));
        self::assertNotSame(
            [],
            $unsub,
            'the private inbox must be UNSUBbed from the finally even when the fetch throws a heartbeat miss',
        );
    }

    // kills LogicalNot @ 888 (directGetBatch idle-sleep guard)
    //
    // One data message (terminated by Nats-Num-Pending: 0) split into >1001 one-byte transport chunks,
    // against a ~1001 ms internal deadline (expiresMs=1). Every read consumes a byte without completing
    // the frame, so the real code (idle-sleep ONLY when no bytes were consumed) drains the whole payload
    // in well under the deadline and returns the message. The mutant sleeps 1 ms on every byte-consuming
    // read, accruing >1001 ms before the frame completes, so it stalls and throws instead of returning.
    // Asserting the message IS returned therefore fails on the mutant.
    public function testDirectGetBatchDrainsChunkedMessageWithoutPerChunkIdleSleep(): void
    {
        $body = str_repeat('x', 2000);
        $headers = "NATS/1.0\r\nNats-Stream: ORDERS\r\nNats-Subject: orders.a\r\nNats-Sequence: 5\r\nNats-Num-Pending: 0\r\n\r\n";
        $frame = sprintf(
            "HMSG _INBOX.JS.DGET.x 1 %d %d\r\n%s%s\r\n",
            strlen($headers),
            strlen($headers) + strlen($body),
            $headers,
            $body,
        );

        $chunks = str_split($frame); // one byte per transport read: >2100 partial-progress reads
        self::assertGreaterThan(1001, count($chunks), 'the payload must span more chunks than the ~1001 ms deadline tolerates as 1 ms sleeps');

        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $client = $this->connect($transport);
        foreach ($chunks as $chunk) {
            $transport->pushReadChunk($chunk);
        }

        $messages = $client->jetStream()->directGetBatch('ORDERS', ['batch' => 10], 1)->await();

        self::assertCount(1, $messages, 'the chunked batch message must be drained and returned, not lost to a per-chunk idle sleep');
        self::assertSame($body, $messages[0]->payload);
    }

    // kills LogicalNot @ 2097 (fetchBatch idle-sleep guard)
    //
    // Same shape as the directGetBatch case, through fetchBatch: one message split into >1001 one-byte
    // chunks against a ~1001 ms deadline (expiresMs=1, batch=1). The real path drains it fast and
    // returns the message; the 1 ms-per-chunk mutant blows the deadline with the frame still incomplete,
    // breaks with an empty batch, and throws the 408 "no messages" error instead of returning.
    public function testFetchBatchDrainsChunkedMessageWithoutPerChunkIdleSleep(): void
    {
        $body = str_repeat('y', 2000);
        $frame = sprintf("MSG _INBOX.JS.FETCH.a 1 %d\r\n%s\r\n", strlen($body), $body);

        $chunks = str_split($frame);
        self::assertGreaterThan(1001, count($chunks), 'the payload must span more chunks than the ~1001 ms deadline tolerates as 1 ms sleeps');

        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $client = $this->connect($transport);
        foreach ($chunks as $chunk) {
            $transport->pushReadChunk($chunk);
        }

        $messages = $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 1)->await();

        self::assertCount(1, $messages, 'the chunked fetch message must be drained and returned, not lost to a per-chunk idle sleep');
        self::assertSame($body, $messages[0]->payload);
    }
}
