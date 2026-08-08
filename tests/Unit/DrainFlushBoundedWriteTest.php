<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Tests\Support\WedgedWriteTransport;
use PHPUnit\Framework\TestCase;

/**
 * drain() and flush() document bounded completion (~requestTimeoutMs), but their WRITES used to be
 * unbounded: a peer stalling with a full send buffer suspends transport writes indefinitely, and
 * drain() cancels the heartbeat FIRST - removing the only escalation that could break the wedge -
 * so a drain against such a peer hung forever in violation of its own contract (#149's write-phase
 * twin). The writes' waits are now bounded by the same budget; the wedged write is abandoned and
 * the teardown's socket close fails it out.
 */
final class DrainFlushBoundedWriteTest extends TestCase
{
    private const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    /**
     * A wedged UNSUB write must not hang drain(): the drain budget bounds the write wait, the flush
     * phase is skipped, and the connection still reaches Closed with the transport closed.
     */
    public function testDrainCompletesWithinBudgetWhenWritesWedge(): void
    {
        // Writes 1-3 succeed (CONNECT, PING, SUB); the drain-phase UNSUB (4th) wedges.
        $transport = new WedgedWriteTransport([self::INFO, "PONG\r\n"], wedgeAfterWrites: 3);
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 300, pingIntervalSeconds: 0), $transport);
        $client->connect()->await();
        $client->subscribe('orders.>', static function (NatsMessage $m): void {})->await();

        $startedNs = hrtime(true);
        // Pre-fix this never returned (the drain fiber parked inside the UNSUB write with the
        // heartbeat already cancelled); the outer cancellation turns a regression into a bounded
        // failure instead of a hung run.
        $client->drain()->await(new TimeoutCancellation(5.0));
        $elapsedSeconds = (hrtime(true) - $startedNs) / 1e9;

        self::assertSame(ConnectionState::Closed, $client->state(), 'drain must always reach Closed');
        self::assertLessThan(3.0, $elapsedSeconds, 'drain must complete within its ~requestTimeoutMs budget, not hang');
    }

    /**
     * A wedged PING write must not hang flush(): it times out within the request budget with the
     * documented TimeoutException instead of parking forever before the read phase even starts.
     */
    public function testFlushTimesOutWhenPingWriteWedges(): void
    {
        // Writes 1-2 succeed (CONNECT, PING handshake); flush()'s PING (3rd) wedges.
        $transport = new WedgedWriteTransport([self::INFO, "PONG\r\n"], wedgeAfterWrites: 2);
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 300, pingIntervalSeconds: 0), $transport);
        $client->connect()->await();

        $startedNs = hrtime(true);
        try {
            // Pre-fix this suspended forever inside the write; the outer cancellation bounds a
            // regression to a clean failure.
            $client->flush()->await(new TimeoutCancellation(5.0));
            self::fail('flush() against a wedged transport must time out');
        } catch (TimeoutException $e) {
            self::assertStringContainsString('backpressure', $e->getMessage());
        }
        $elapsedSeconds = (hrtime(true) - $startedNs) / 1e9;

        self::assertLessThan(3.0, $elapsedSeconds, 'flush must fail within its request-timeout budget');

        // The connection is still usable state-wise; disconnect releases the wedged write cleanly.
        $client->disconnect()->await(new TimeoutCancellation(5.0));
        self::assertSame(ConnectionState::Closed, $client->state());
    }
}
