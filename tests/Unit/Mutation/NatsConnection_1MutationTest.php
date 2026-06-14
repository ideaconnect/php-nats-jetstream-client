<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use Amp\DeferredFuture;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\FlakyTransport;
use SplQueue;

use function Amp\async;
use function Amp\delay;

/**
 * Targeted unit tests whose sole purpose is to kill Infection mutants surviving in
 * src/Connection/NatsConnection.php (chunk 1). Each assertion group is annotated with the
 * mutator and source line it pins.
 */
final class NatsConnection_1MutationTest extends \PHPUnit\Framework\TestCase
{
    private const HANDSHAKE_INFO
        = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    /**
     * @param list<string> $reads
     *
     * @param-out FakeTransport $transport
     */
    private function openConnection(array $reads = [], ?NatsOptions $options = null, ?FakeTransport &$transport = null): NatsConnection
    {
        $transport = new FakeTransport(array_merge([self::HANDSHAKE_INFO, "PONG\r\n"], $reads));
        $connection = new NatsConnection($options ?? new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        return $connection;
    }

    private static function getProp(object $obj, string $name): mixed
    {
        return (new \ReflectionProperty($obj::class, $name))->getValue($obj);
    }

    private static function setProp(object $obj, string $name, mixed $value): void
    {
        (new \ReflectionProperty($obj::class, $name))->setValue($obj, $value);
    }

    // kills NullSafePropertyCall @ line 189
    public function testMaxPayloadReturnsNullBeforeServerInfoIsKnown(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        // serverInfo is null before connect; the null-safe operator must yield null, not dereference it.
        self::assertNull($connection->maxPayload());
    }

    // kills Throw_ @ line 215, MethodCallRemoval @ line 219
    public function testRttThrowsWhenNotOpenAndFlushesAPingWhenOpen(): void
    {
        // rtt() on a connection that was never opened must throw (line 215's throw is masked by flush()'s
        // own identical guard, so this pins the observable contract rather than that specific mutant).
        $closed = new NatsConnection(new NatsOptions(), new FakeTransport());
        $threw = false;
        try {
            $closed->rtt()->await();
        } catch (ConnectionException $e) {
            $threw = true;
            self::assertSame('Connection is not open', $e->getMessage());
        }
        self::assertTrue($threw, 'rtt() must throw when not open');

        // MethodCallRemoval @ 219: rtt() must drive flush(), which writes a PING and awaits the PONG.
        $connection = $this->openConnection(["PONG\r\n"], transport: $transport);
        $rtt = $connection->rtt()->await();

        self::assertGreaterThanOrEqual(0.0, $rtt);
        // writes: [0]=CONNECT, [1]=handshake PING, [2]=flush PING from rtt(). Without flush() no 3rd PING.
        self::assertSame("PING\r\n", $transport->writes[2]);
    }

    // kills MethodCallRemoval @ line 257
    public function testAuthFailureEmitsClosedEvent(): void
    {
        $events = [];
        $transport = new FakeTransport([
            self::HANDSHAKE_INFO,
            "-ERR Authorization Violation\r\n",
        ]);
        $connection = new NatsConnection(
            new NatsOptions(connectionListener: static function (ConnectionEvent $e) use (&$events): void {
                $events[] = $e;
            }),
            $transport,
        );

        try {
            $connection->connect()->await();
            self::fail('expected auth failure');
        } catch (\Throwable) {
            // expected
        }

        // The Closed lifecycle event must be emitted on the auth-failure path.
        self::assertContains(ConnectionEvent::Closed, $events);
    }

    // kills GreaterThan @ line 261
    public function testReconnectEnabledWithZeroAttemptsDoesNotRecoverAndThrows(): void
    {
        // maxReconnectAttempts == 0 is the boundary: `> 0` is false, so the connect must NOT enter
        // recovery and must surface the failure. `>= 0` (mutant) would swallow it via recoverConnection().
        $transport = new FlakyTransport([[self::HANDSHAKE_INFO, "PONG\r\n"]], connectFailures: 1);
        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 0,
                retryOnFailedInitialConnect: false,
            ),
            $transport,
        );

        $this->expectException(ConnectionException::class);
        $connection->connect()->await();
    }

    // kills GreaterThan @ line 270
    public function testRetryInitialConnectWithZeroAttemptsDoesNotRetryAndThrows(): void
    {
        // retryOnFailedInitialConnect path, boundary maxReconnectAttempts == 0: `> 0` false -> throw,
        // never invoking the retry loop. `>= 0` (mutant) would retry (and succeed) instead.
        $transport = new FlakyTransport([[self::HANDSHAKE_INFO, "PONG\r\n"]], connectFailures: 1);
        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: false,
                retryOnFailedInitialConnect: true,
                maxReconnectAttempts: 0,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
            ),
            $transport,
        );

        $this->expectException(ConnectionException::class);
        try {
            $connection->connect()->await();
        } finally {
            // Exactly one connect attempt: the boundary blocks any retry.
            self::assertCount(1, $transport->connectCalls);
        }
    }

    // kills MethodCallRemoval @ line 277
    public function testGeneralConnectFailureEmitsClosedEvent(): void
    {
        $events = [];
        // A bad control line during handshake -> ConnectionException with reconnect disabled.
        $transport = new FakeTransport([self::HANDSHAKE_INFO, "UNKNOWN\r\n"]);
        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: false,
                retryOnFailedInitialConnect: false,
                connectionListener: static function (ConnectionEvent $e) use (&$events): void {
                    $events[] = $e;
                },
            ),
            $transport,
        );

        try {
            $connection->connect()->await();
            self::fail('expected connect failure');
        } catch (ConnectionException) {
            // expected
        }

        self::assertContains(ConnectionEvent::Closed, $events);
    }

    // kills MethodCallRemoval @ line 294 (disconnect cancels ping timer)
    public function testDisconnectCancelsPingTimer(): void
    {
        $connection = $this->openConnection(options: new NatsOptions(pingIntervalSeconds: 30));

        // A heartbeat timer is armed while open.
        self::assertNotNull(self::getProp($connection, 'pingTimerId'));

        $connection->disconnect()->await();

        // disconnect() must cancel it (id cleared); otherwise a stale repeating timer leaks.
        self::assertNull(self::getProp($connection, 'pingTimerId'));
    }

    // kills MethodCallRemoval @ line 326 (drain cancels ping timer) and
    // TrueValue @ line 325 (drain latches close-intent) and
    // MethodCallRemoval @ line 335 (drain writes the flush PING) and
    // FalseValue @ line 373 (drainFlushPending cleared at end of drain)
    public function testDrainLatchesCloseIntentCancelsTimerWritesPingAndClearsFlushFlag(): void
    {
        $connection = $this->openConnection(
            ["PONG\r\n"],
            options: new NatsOptions(pingIntervalSeconds: 30),
            transport: $transport,
        );

        self::assertNotNull(self::getProp($connection, 'pingTimerId'));

        $connection->drain()->await();

        // TrueValue @ 325: close-intent is latched so a mid-drain recovery cannot re-open.
        self::assertTrue(self::getProp($connection, 'closing'));
        // MethodCallRemoval @ 326: the ping timer is cancelled.
        self::assertNull(self::getProp($connection, 'pingTimerId'));
        // MethodCallRemoval @ 335: a flush PING is written during drain.
        self::assertContains("PING\r\n", array_slice($transport->writes, 2));
        // FalseValue @ 373: the drain-flush flag is cleared at the end.
        self::assertFalse(self::getProp($connection, 'drainFlushPending'));
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    // kills MethodCallRemoval @ line 367 (drain delivers remaining buffered messages)
    public function testDrainDeliversBufferedMessagesNotDrainedByTheFlushLoop(): void
    {
        // After the flush loop ends (here by deadline, having drained nothing), drain() must still hand
        // any buffered message to its callback. Without that final drainAllPending() it is silently lost.
        $transport = new FakeTransport([self::HANDSHAKE_INFO, "PONG\r\n"], blockWhenEmpty: true);
        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0, requestTimeoutMs: 1), $transport);
        $connection->connect()->await();

        $delivered = [];
        $sid = $connection->subscribe('updates', static function (NatsMessage $m) use (&$delivered): void {
            $delivered[] = $m->payload;
        })->await();

        // Buffer a message that the flush loop will never read (the socket blocks until the deadline).
        $queue = new SplQueue();
        $queue->enqueue(new NatsMessage('updates', $sid, null, 'buffered'));
        $pending = self::getProp($connection, 'pendingMessages');
        $pending[$sid] = $queue;
        self::setProp($connection, 'pendingMessages', $pending);

        $connection->drain()->await();

        self::assertSame(['buffered'], $delivered);
    }

    // kills MethodCallRemoval @ line 402 (record outbound on buffered publish) and
    // ReturnRemoval @ line 404 (return after buffering — a fall-through writes + records twice)
    public function testBufferedPublishRecordsOutboundExactlyOnceAndDoesNotWriteImmediately(): void
    {
        $connection = $this->openConnection(transport: $transport);

        // Simulate "reconnect in flight": state != Open and an active reconnect future, so publish buffers.
        self::setProp($connection, 'reconnecting', new DeferredFuture());
        self::setProp($connection, 'state', ConnectionState::Connecting);

        $writesBefore = count($transport->writes);
        $connection->publish('a.b', 'buffered')->await();

        $stats = $connection->statistics();
        // @ 402: outbound was recorded (1, not 0). @ 404: recorded exactly once (1, not 2) and the frame
        // was buffered, not written to the transport immediately.
        self::assertSame(1, $stats->outMsgs);
        self::assertSame(strlen('buffered'), $stats->outBytes);
        self::assertSame($writesBefore, count($transport->writes));
        // The buffered frame is held in the reconnect buffer.
        self::assertSame("PUB a.b 8\r\nbuffered\r\n", self::getProp($connection, 'reconnectBuffer'));
    }

    // kills MethodCallRemoval @ line 432 (publishWithHeaders validates the subject)
    public function testPublishWithHeadersValidatesSubject(): void
    {
        $connection = $this->openConnection();

        $this->expectException(ProtocolException::class);
        // A subject containing whitespace must be rejected before any frame is built/sent.
        $connection->publishWithHeaders('bad subject', 'p', ['X' => 'y'])->await();
    }

    // line 470 LessThanOrEqualTo is equivalent (the line-474 size check rejects any non-empty frame
    // at bufferSize 0 too), so this pins the documented behavior rather than that specific mutant.
    public function testZeroReconnectBufferSizeDisablesBufferingDuringReconnect(): void
    {
        $connection = $this->openConnection(options: new NatsOptions(pingIntervalSeconds: 0, reconnectBufferSize: 0));

        self::setProp($connection, 'reconnecting', new DeferredFuture());
        self::setProp($connection, 'state', ConnectionState::Connecting);

        // bufferSize == 0: `<= 0` is true -> bufferFrame returns false -> publish throws. `< 0` (mutant)
        // would treat 0 as "buffering enabled" and swallow the publish.
        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Connection is not open');
        $connection->publish('a.b', 'x')->await();
    }

    // kills GreaterThan @ line 474 (buffer-full boundary: sum == bufferSize must still buffer)
    public function testBufferFrameAcceptsFrameThatExactlyFillsTheBuffer(): void
    {
        // Frame "PUB a.b 1\r\nX\r\n" has a fixed length; size the buffer to exactly that. `> size` is
        // false at equality so it buffers; `>= size` (mutant) would reject it as overflow and throw.
        $frame = "PUB a.b 1\r\nX\r\n";
        $connection = $this->openConnection(
            options: new NatsOptions(pingIntervalSeconds: 0, reconnectBufferSize: strlen($frame)),
        );

        self::setProp($connection, 'reconnecting', new DeferredFuture());
        self::setProp($connection, 'state', ConnectionState::Connecting);

        // Must NOT throw: the frame exactly fits.
        $connection->publish('a.b', 'X')->await();

        self::assertSame($frame, self::getProp($connection, 'reconnectBuffer'));
    }

    // kills Assignment @ line 489 (outBytes += vs =)
    public function testRecordOutboundAccumulatesBytesAcrossPublishes(): void
    {
        $connection = $this->openConnection(transport: $transport);

        $connection->publish('a.b', 'abc')->await();      // 3 bytes
        $connection->publish('a.b', 'defghij')->await();  // 7 bytes

        $stats = $connection->statistics();
        self::assertSame(2, $stats->outMsgs);
        // `+=` accumulates to 10; `=` (mutant) would leave only the last payload's 7.
        self::assertSame(10, $stats->outBytes);
    }

    // kills MethodCallRemoval @ line 549 (drainSubscription drops local state when not open)
    public function testDrainSubscriptionDropsLocalStateWhenNotOpen(): void
    {
        $connection = $this->openConnection();
        $sid = $connection->subscribe('updates', static function (NatsMessage $m): void {})->await();

        self::assertArrayHasKey($sid, self::getProp($connection, 'subscriptionMeta'));

        // Force "not open" while keeping the subscription state present.
        self::setProp($connection, 'state', ConnectionState::Closed);

        $connection->drainSubscription($sid)->await();

        // The local subscription state must be dropped on the not-open branch.
        self::assertArrayNotHasKey($sid, self::getProp($connection, 'subscriptionMeta'));
        self::assertArrayNotHasKey($sid, self::getProp($connection, 'subscriptions'));
    }

    // kills MethodCallRemoval @ line 567 (drainSubscription delivers in-flight buffered messages)
    public function testDrainSubscriptionDeliversBufferedMessageWhenFlushTimesOut(): void
    {
        // The flush() in drainSubscription times out (socket blocks), so the only delivery path for a
        // buffered message is the explicit drainPendingForSid() at line 567.
        $transport = new FakeTransport([self::HANDSHAKE_INFO, "PONG\r\n"], blockWhenEmpty: true);
        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0, requestTimeoutMs: 1), $transport);
        $connection->connect()->await();

        $delivered = [];
        $sid = $connection->subscribe('updates', static function (NatsMessage $m) use (&$delivered): void {
            $delivered[] = $m->payload;
        })->await();

        $queue = new SplQueue();
        $queue->enqueue(new NatsMessage('updates', $sid, null, 'inflight'));
        $pending = self::getProp($connection, 'pendingMessages');
        $pending[$sid] = $queue;
        self::setProp($connection, 'pendingMessages', $pending);

        $connection->drainSubscription($sid)->await();

        self::assertSame(['inflight'], $delivered);
    }

    // kills Throw_ @ line 584 (flush throws when not open) and MethodCallRemoval @ line 588 (flush writes PING)
    public function testFlushThrowsWhenNotOpenAndWritesPingWhenOpen(): void
    {
        // Throw_ @ 584: the not-open guard must throw BEFORE any side effect. Without the throw the
        // method falls through and writes a PING (then processIncoming's guard throws the same type),
        // so we pin "no bytes written on the not-open path" to distinguish the two.
        $closedTransport = new FakeTransport();
        $closed = new NatsConnection(new NatsOptions(), $closedTransport);
        $threw = false;
        try {
            $closed->flush()->await();
        } catch (ConnectionException $e) {
            $threw = true;
            self::assertSame('Connection is not open', $e->getMessage());
        }
        self::assertTrue($threw, 'flush() must throw when not open');
        self::assertSame([], $closedTransport->writes, 'flush() must not write before the not-open guard');

        // MethodCallRemoval @ 588: flush writes a PING (then awaits the PONG we feed).
        $connection = $this->openConnection(["PONG\r\n"], transport: $transport);
        $connection->flush()->await();
        self::assertSame("PING\r\n", $transport->writes[2]);
    }

    // kills UnwrapFinally @ line 591 and Finally_ @ line 603 (flushPending cleared even on timeout)
    public function testFlushPendingClearedAfterTimeout(): void
    {
        $transport = new FakeTransport([self::HANDSHAKE_INFO, "PONG\r\n"], blockWhenEmpty: true);
        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0, requestTimeoutMs: 1), $transport);
        $connection->connect()->await();

        try {
            $connection->flush()->await();
            self::fail('expected flush timeout');
        } catch (\Throwable) {
            // expected (TimeoutException)
        }

        // The finally block must clear flushPending even when the timeout propagates.
        self::assertFalse(self::getProp($connection, 'flushPending'));
    }

    // kills FalseValue @ line 604 (flushPending stays false after a successful flush)
    public function testFlushPendingFalseAfterSuccessfulFlush(): void
    {
        $connection = $this->openConnection(["PONG\r\n"]);
        $connection->flush()->await();

        // After a successful flush the finally must leave flushPending false (mutant sets it true).
        self::assertFalse(self::getProp($connection, 'flushPending'));
    }

    // kills ReturnRemoval @ line 629 (concurrent-read guard returns 0 without reading)
    public function testProcessIncomingReturnsZeroWhenReadAlreadyInProgress(): void
    {
        $connection = $this->openConnection(["MSG updates 1 5\r\nhello\r\n"], transport: $transport);
        $received = [];
        $connection->subscribe('updates', static function (NatsMessage $m) use (&$received): void {
            $received[] = $m->payload;
        })->await();

        // Pretend a concurrent read owns the socket.
        self::setProp($connection, 'readInProgress', true);

        // Guard must short-circuit to 0 WITHOUT consuming the queued frame.
        $frames = $connection->processIncoming()->await();
        self::assertSame(0, $frames);
        self::assertSame([], $received);

        // Release the guard: the still-queued frame is now read and dispatched.
        self::setProp($connection, 'readInProgress', false);
        self::assertSame(1, $connection->processIncoming()->await());
        self::assertSame(['hello'], $received);
    }

    // kills TrueValue @ line 632 (readInProgress is SET true so a concurrent read is excluded)
    public function testProcessIncomingMarksReadInProgressToExcludeConcurrentRead(): void
    {
        $transport = new FakeTransport([self::HANDSHAKE_INFO, "PONG\r\n"], blockWhenEmpty: true);
        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        // Read #1 enters the blocking socket read; it must set readInProgress = true.
        $read1 = async(static fn(): int => $connection->processIncoming()->await());
        // Cooperatively let read1 enter readLine() and latch the in-progress flag.
        while ($transport->startedReads < 1) {
            delay(0.001);
        }

        self::assertTrue(self::getProp($connection, 'readInProgress'), 'first read must latch readInProgress');

        // Read #2 must be excluded by the guard and return 0 immediately. If 632 set the flag false,
        // read2 would instead enter a second blocking read and its bounded cancellation would throw.
        $read2 = $connection->processIncoming(new TimeoutCancellation(1.0))->await();
        self::assertSame(0, $read2);

        $read1->ignore();
    }

    // kills MethodCallRemoval @ line 643 (read failure surfaces to the error listener)
    public function testReadFailureEmitsErrorToListener(): void
    {
        $errors = [];
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [self::HANDSHAKE_INFO, "PONG\r\n", '__THROW__'],
                [self::HANDSHAKE_INFO, "PONG\r\n"],
            ],
            connectFailures: 0,
            readFailures: 0,
        );
        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 3,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
                errorListener: static function (\Throwable $e) use (&$errors): void {
                    $errors[] = $e->getMessage();
                },
            ),
            $transport,
        );
        $connection->connect()->await();

        // The read throws -> recovery runs, but the error must first be surfaced to the listener.
        $connection->processIncoming()->await();

        self::assertNotEmpty($errors, 'read failure must be reported to the error listener');
    }

    // kills ReturnRemoval @ line 653 (empty read returns 0 without draining buffered messages)
    public function testEmptyReadDoesNotDeliverBufferedMessages(): void
    {
        // FakeTransport returns '' when its queue is exhausted (blockWhenEmpty=false). On that empty
        // read processIncoming must return early WITHOUT running drainAllPending(); otherwise a buffered
        // message would be delivered on a read that produced no bytes.
        $connection = $this->openConnection();
        $sid = $connection->subscribe('updates', static function (NatsMessage $m): void {
            self::fail('buffered message must not be delivered on an empty read');
        })->await();

        $queue = new SplQueue();
        $queue->enqueue(new NatsMessage('updates', $sid, null, 'x'));
        $pending = self::getProp($connection, 'pendingMessages');
        $pending[$sid] = $queue;
        self::setProp($connection, 'pendingMessages', $pending);

        // Queue exhausted -> readLine() returns '' -> early return 0, no delivery.
        $frames = $connection->processIncoming()->await();
        self::assertSame(0, $frames);

        // The message is still buffered (not delivered).
        $stillPending = self::getProp($connection, 'pendingMessages');
        self::assertArrayHasKey($sid, $stillPending);
        self::assertFalse($stillPending[$sid]->isEmpty());
    }
}
