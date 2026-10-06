<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\LoopTickCountingTransport;
use IDCT\NATS\Transport\TransportInterface;
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
 *    split into one-byte chunks through a transport that counts the chunks read after the event loop
 *    ran a tick ({@see LoopTickCountingTransport}): the real path reads them back to back and counts
 *    none, whereas each of the mutant's sleeps lets the loop tick, so every chunk after the first
 *    counts. The count does not depend on how fast the machine is (#176).
 *
 * All frames are driven through the in-process FakeTransport (no sockets, no Docker); the collect
 * loop itself pumps the reads.
 */
final class JetStreamContext_7MutationTest extends TestCase
{
    private const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    private function connect(TransportInterface $transport): NatsClient
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
    // One data message (terminated by Nats-Num-Pending: 0) split into one-byte transport chunks. Every
    // read consumes a byte without completing the frame, so the real code (idle-sleep ONLY when no bytes
    // were consumed) reads chunk after chunk without letting the event loop tick between them, and
    // returns the message. The mutant sleeps 1 ms on every byte-consuming read, and the loop ticks during
    // each sleep: the transport counts every chunk after the first as read after a tick, where the real
    // code counts none. The 6 s progress bound (expiresMs=5000) gives the real code all the time it needs,
    // however slow the machine is (#176).
    public function testDirectGetBatchDrainsChunkedMessageWithoutPerChunkIdleSleep(): void
    {
        $body = str_repeat('x', 300);
        $headers = "NATS/1.0\r\nNats-Stream: ORDERS\r\nNats-Subject: orders.a\r\nNats-Sequence: 5\r\nNats-Num-Pending: 0\r\n\r\n";
        $frame = sprintf(
            "HMSG _INBOX.JS.DGET.x 1 %d %d\r\n%s%s\r\n",
            strlen($headers),
            strlen($headers) + strlen($body),
            $headers,
            $body,
        );

        $chunks = str_split($frame); // one byte per transport read: hundreds of partial-progress reads

        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $counting = new LoopTickCountingTransport($transport);
        $client = $this->connect($counting);
        foreach ($chunks as $chunk) {
            $transport->pushReadChunk($chunk);
        }
        $counting->reset(); // count the message's chunks only, not the handshake's

        $messages = $client->jetStream()->directGetBatch('ORDERS', ['batch' => 10], 5_000)->await();

        self::assertCount(1, $messages, 'the chunked batch message must be drained and returned');
        self::assertSame($body, $messages[0]->payload);
        self::assertSame(count($chunks), $counting->chunksRead, 'every byte of the message must be read as its own chunk');
        self::assertSame(
            0,
            $counting->chunksReadAfterALoopTick,
            'the collect loop must read the next chunk at once, not idle-sleep 1 ms per partial chunk',
        );
    }

    // kills LogicalNot @ 2097 (fetchBatch idle-sleep guard)
    //
    // Same shape as the directGetBatch case, through fetchBatch (batch=1): one message split into
    // one-byte chunks. The real path reads the chunks back to back and returns the message; each of the
    // 1 ms-per-chunk mutant's sleeps lets the event loop tick, so the transport counts every chunk after
    // the first as read after a tick. The 6 s deadline (expiresMs=5000) gives the real code all the time
    // it needs, however slow the machine is (#176).
    public function testFetchBatchDrainsChunkedMessageWithoutPerChunkIdleSleep(): void
    {
        $body = str_repeat('y', 300);
        $frame = sprintf("MSG _INBOX.JS.FETCH.a 1 %d\r\n%s\r\n", strlen($body), $body);

        $chunks = str_split($frame); // one byte per transport read: hundreds of partial-progress reads

        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $counting = new LoopTickCountingTransport($transport);
        $client = $this->connect($counting);
        foreach ($chunks as $chunk) {
            $transport->pushReadChunk($chunk);
        }
        $counting->reset(); // count the message's chunks only, not the handshake's

        $messages = $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 5_000)->await();

        self::assertCount(1, $messages, 'the chunked fetch message must be drained and returned');
        self::assertSame($body, $messages[0]->payload);
        self::assertSame(count($chunks), $counting->chunksRead, 'every byte of the message must be read as its own chunk');
        self::assertSame(
            0,
            $counting->chunksReadAfterALoopTick,
            'the collect loop must read the next chunk at once, not idle-sleep 1 ms per partial chunk',
        );
    }
}
