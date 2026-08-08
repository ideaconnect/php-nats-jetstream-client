<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\WedgedWriteTransport;
use IDCT\NATS\Transport\TransportClosedException;
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

    /**
     * A wedged flush-phase PING write (no subscriptions, so the first drain write is the PING) must
     * not hang drain() either: the wedge surfaces as a cancellation through the PING write's own
     * catch (the slot deliberately stays queued - the bytes may still reach the wire), the flush
     * phase is skipped, and drain still reaches Closed within its budget.
     */
    public function testDrainSkipsFlushPhaseWhenPingWriteWedges(): void
    {
        // Writes 1-2 succeed (CONNECT, handshake PING); with no subscriptions the drain-phase
        // flush PING (3rd) is the first drain write - and wedges.
        $transport = new WedgedWriteTransport([self::INFO, "PONG\r\n"], wedgeAfterWrites: 2);
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 300, pingIntervalSeconds: 0), $transport);
        $client->connect()->await();

        $startedNs = hrtime(true);
        $client->drain()->await(new TimeoutCancellation(5.0));
        $elapsedSeconds = (hrtime(true) - $startedNs) / 1e9;

        self::assertSame(ConnectionState::Closed, $client->state(), 'drain must always reach Closed');
        self::assertLessThan(3.0, $elapsedSeconds, 'a wedged flush PING must not hang drain past its budget');
    }

    /**
     * Disclosed contract (#150): drain() no longer THROWS on a dead-socket write failure. Here the
     * flush-phase PING write fails outright (dead socket, write throws): drain()->await() must
     * resolve, the failure must surface via the error listener (it was silently swallowed - or
     * thrown, stranding Draining - before), and the connection must still reach Closed.
     */
    public function testDrainReportsDeadSocketPingWriteFailureAndStillCloses(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $errors = [];
        $client = new NatsClient(
            new NatsOptions(
                requestTimeoutMs: 300,
                pingIntervalSeconds: 0,
                errorListener: static function (\Throwable $e) use (&$errors): void {
                    $errors[] = $e;
                },
            ),
            $transport,
        );
        $client->connect()->await();

        // Arm AFTER the handshake so only the drain-phase PING write dies.
        $transport->throwOnWriteContaining = 'PING';

        // Pre-fix (or on regression) this throws and leaves state stranded in Draining.
        $client->drain()->await(new TimeoutCancellation(5.0));

        self::assertSame(ConnectionState::Closed, $client->state(), 'drain must reach Closed despite the dead socket');
        self::assertTrue($transport->closed, 'the socket must still be torn down');
        $writeFailures = array_filter($errors, static fn(\Throwable $e): bool => $e instanceof TransportClosedException);
        self::assertNotSame([], $writeFailures, 'the dead-socket write failure must surface via the error listener');
    }

    /**
     * Same disclosed contract for the per-sid UNSUB writes: pre-#150 they ran OUTSIDE the drain
     * try, so a dead-socket UNSUB write threw out of drain() and stranded the connection in
     * Draining with the socket open. The write failure must be reported and drain must still close.
     */
    public function testDrainReportsDeadSocketUnsubWriteFailureAndStillCloses(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $errors = [];
        $client = new NatsClient(
            new NatsOptions(
                requestTimeoutMs: 300,
                pingIntervalSeconds: 0,
                errorListener: static function (\Throwable $e) use (&$errors): void {
                    $errors[] = $e;
                },
            ),
            $transport,
        );
        $client->connect()->await();
        $client->subscribe('orders.>', static function (NatsMessage $message): void {})->await();

        $transport->throwOnWriteContaining = 'UNSUB';

        $client->drain()->await(new TimeoutCancellation(5.0));

        self::assertSame(ConnectionState::Closed, $client->state(), 'a failed UNSUB write must not strand drain in Draining');
        $writeFailures = array_filter($errors, static fn(\Throwable $e): bool => $e instanceof TransportClosedException);
        self::assertNotSame([], $writeFailures, 'the UNSUB write failure must surface via the error listener');
    }

    /**
     * The write-failure report itself is containment-hardened (#158): emitError() logs BEFORE its
     * listener-throw guard, so a throwing user-supplied PSR logger would otherwise escape
     * emitErrorSafely's caller and break drain's teardown. drain must still resolve and close.
     */
    public function testDrainStillClosesWhenLoggerThrowsWhileReportingWriteFailure(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);

        // Throws only while the drain write failure is reported - a logger throwing on every call
        // would already fail the connect() handshake logging and miss the point of the test.
        $throwingLogger = new class extends \Psr\Log\AbstractLogger {
            public function log($level, \Stringable|string $message, array $context = []): void
            {
                if (str_contains((string) $message, 'Simulated write failure')) {
                    throw new \RuntimeException('logger failed');
                }
            }
        };

        $client = new NatsClient(
            new NatsOptions(requestTimeoutMs: 300, pingIntervalSeconds: 0, logger: $throwingLogger),
            $transport,
        );
        $client->connect()->await();

        $transport->throwOnWriteContaining = 'PING';

        // On regression (emitErrorSafely rethrowing) the logger's RuntimeException escapes here.
        $client->drain()->await(new TimeoutCancellation(5.0));

        self::assertSame(ConnectionState::Closed, $client->state(), 'a throwing logger must not break drain teardown');
        self::assertTrue($transport->closed);
    }

    /**
     * A handler ack/reply published while drain delivers backlog rides a socket whose heartbeat is
     * already cancelled, so its write wait is bounded like drain's own writes: when the transport
     * wedges (backpressure), the publisher gets the documented TimeoutException instead of parking
     * forever - and drain still reaches Closed.
     */
    public function testPublishDuringDrainTimesOutWhenWriteWedges(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        // The backlog MSG (and the flush PONG behind it) arrive only once drain's UNSUB hits the
        // wire, so the handler publish below runs INSIDE the drain flush phase, state Draining.
        $transport->enqueueOnWriteContaining = [
            'UNSUB' => ["MSG updates 1 5\r\nhello\r\n", "PONG\r\n"],
        ];

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 300, pingIntervalSeconds: 0), $transport);
        $client->connect()->await();

        $received = [];
        $captured = null;
        $client->subscribe('updates', static function (NatsMessage $message) use ($client, &$received, &$captured): void {
            $received[] = $message->payload;
            try {
                $client->publish('updates.reply', 'ok')->await();
            } catch (\Throwable $e) {
                $captured = $e;
            }
        })->await();

        // Only the handler's mid-drain PUB wedges; drain's own UNSUB/PING writes stay healthy.
        $transport->wedgeOnWriteContaining = 'PUB ';

        $client->drain()->await(new TimeoutCancellation(5.0));

        self::assertSame(['hello'], $received, 'the backlog message must be delivered during drain');
        self::assertStringContainsString('PUB updates.reply', implode('', $transport->writes), 'the ack must have been attempted on the wire');
        self::assertInstanceOf(TimeoutException::class, $captured, 'a wedged mid-drain publish must fail with the documented TimeoutException');
        self::assertStringContainsString('Publish during drain timed out', $captured->getMessage());
        self::assertSame(ConnectionState::Closed, $client->state(), 'drain must still reach Closed');
    }
}
