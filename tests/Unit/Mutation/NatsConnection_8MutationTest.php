<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use Amp\DeferredCancellation;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\IncomingChunkResult;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\Future\await;

/**
 * Kills surviving Infection mutants in {@see NatsConnection}'s read path (#119): the three
 * IncomingChunkResult return-flag mutants (readInProgress / read-error / parse-error branches) and
 * the requestMany() wait-loop idle-sleep guard. Each test pins the exact observable behavior the
 * mutation would change (the consumedBytes flag a wait loop reads, or the frame drained without a
 * per-partial-chunk 1 ms sleep).
 */
final class NatsConnection_8MutationTest extends TestCase
{
    /** @return list<string> */
    private function infoAndPong(): array
    {
        return [
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ];
    }

    private function connect(FakeTransport $transport, ?NatsOptions $options = null): NatsConnection
    {
        $connection = new NatsConnection($options ?? new NatsOptions(), $transport);
        $connection->connect()->await();

        return $connection;
    }

    /**
     * When another fiber already owns the socket read ($readInProgress), a second readIncoming() must
     * report a GENUINELY IDLE result (consumedBytes=false) so a wait loop yields 1 ms rather than
     * spinning against a read it never actually performed.
     *
     * kills FalseValue @ line 1365: mutation returns IncomingChunkResult(0, true) here, which would
     * tell a wait loop it "made wire progress" while another fiber holds the socket - a busy spin.
     */
    public function testReadInProgressReportsIdleNotConsumed(): void
    {
        // blockWhenEmpty: the first read parks on an empty socket, holding $readInProgress=true.
        $transport = new FakeTransport($this->infoAndPong(), blockWhenEmpty: true);
        $connection = $this->connect($transport);

        // Start a first read that seizes the read slot and parks (queue is drained after connect).
        // FIFO coroutine scheduling guarantees this fiber sets $readInProgress=true and suspends in
        // transport->readLine() before the second read below runs.
        $gate = new DeferredCancellation();
        $parkedRead = $connection->readIncoming($gate->getCancellation());
        $parkedRead->ignore();

        try {
            // The second read observes $readInProgress and takes the early-return branch at line 1365.
            $result = $connection->readIncoming()->await();

            self::assertInstanceOf(IncomingChunkResult::class, $result);
            self::assertSame(0, $result->frames, 'a skipped read completes no frames');
            self::assertFalse(
                $result->consumedBytes,
                'a read skipped because another fiber owns the socket is idle, not wire progress',
            );
        } finally {
            // Unwind the parked first read so no fiber is left suspended past the test.
            $gate->cancel();
            try {
                $parkedRead->await();
            } catch (\Throwable) {
                // Cancelled read: expected on teardown.
            }
        }
    }

    /**
     * A read that FAILS (socket error / EOF) consumed no bytes: readIncoming() must report
     * consumedBytes=false so the recovered caller idle-yields rather than tight-spinning on a read
     * that never landed data.
     *
     * kills FalseValue @ line 1383: mutation returns IncomingChunkResult(0, true) from the read-error
     * catch, falsely claiming wire progress on a failed read.
     */
    public function testReadErrorReportsNotConsumedAfterRecovery(): void
    {
        // Queue: connect (INFO,PONG), then a mid-session EOF that makes readLine() throw, then a fresh
        // INFO,PONG so the triggered recovery reconnects cleanly and readIncoming() returns its result.
        $transport = new FakeTransport(array_merge(
            $this->infoAndPong(),
            [FakeTransport::EOF],
            $this->infoAndPong(),
        ));
        $connection = $this->connect(
            $transport,
            new NatsOptions(reconnectEnabled: true, maxReconnectAttempts: 5, reconnectDelayMs: 1, reconnectJitterMs: 0),
        );

        $result = $connection->readIncoming()->await();

        // The read-error branch ran: recovery reconnected (a second connect() call) and reopened.
        self::assertSame(2, count($transport->connectCalls), 'the read error must trigger a reconnect');
        self::assertSame(ConnectionState::Open, $connection->state());

        self::assertInstanceOf(IncomingChunkResult::class, $result);
        self::assertSame(0, $result->frames, 'a failed read dispatches no frames');
        self::assertFalse(
            $result->consumedBytes,
            'a read that threw consumed no bytes and must report consumedBytes=false',
        );
    }

    /**
     * A read that pushed a non-empty chunk which then failed to PARSE still consumed those bytes off
     * the wire: readIncoming() must report consumedBytes=true so the caller loops immediately instead
     * of paying a 1 ms idle sleep on a read that genuinely made wire progress (#119).
     *
     * kills TrueValue @ line 1429: mutation returns IncomingChunkResult(count, false) from the
     * parse-error catch, falsely marking a byte-consuming read as idle.
     */
    public function testParseErrorReportsConsumedBytesAfterRecovery(): void
    {
        // Queue: connect (INFO,PONG), a corrupt control frame that makes parser->push() throw, then a
        // fresh INFO,PONG so recovery reconnects cleanly and readIncoming() returns its result.
        $transport = new FakeTransport(array_merge(
            $this->infoAndPong(),
            ["GARBAGE\r\n"],
            $this->infoAndPong(),
        ));
        $connection = $this->connect(
            $transport,
            new NatsOptions(reconnectEnabled: true, maxReconnectAttempts: 5, reconnectDelayMs: 1, reconnectJitterMs: 0),
        );

        $result = $connection->readIncoming()->await();

        // The parse-error branch ran: recovery reconnected and reopened.
        self::assertSame(2, count($transport->connectCalls), 'the parse error must trigger a reconnect');
        self::assertSame(ConnectionState::Open, $connection->state());

        self::assertInstanceOf(IncomingChunkResult::class, $result);
        self::assertSame(0, $result->frames, 'no frame parsed before the corrupt control line');
        self::assertTrue(
            $result->consumedBytes,
            'a non-empty chunk was read and pushed, so the parse-error read consumed bytes',
        );
    }

    /**
     * The requestMany() wait loop only idle-sleeps on a GENUINELY idle read: a reply that arrives
     * split across many partial chunks (each consuming bytes but completing no frame yet) must drain
     * immediately, without a 1 ms sleep per chunk (#119).
     *
     * FALSIFIABILITY: the 300-byte reply is fed as ~327 one-byte chunks under a 60 ms request budget.
     * On the real path every partial read reports consumedBytes=true and the guard `if (!consumedBytes)`
     * skips the sleep, so the whole frame drains in a few ms and the reply is collected. The mutation
     * inverts the guard to `if (consumedBytes)`, sleeping 1 ms on every partial chunk (~327 ms) - the
     * 60 ms budget expires with the frame still incomplete and requestMany() returns []. Only the real
     * path collects the reply.
     *
     * kills LogicalNot @ line 1863.
     */
    public function testRequestManyDrainsChunkedReplyWithoutIdleSleepPerPartialChunk(): void
    {
        $transport = new FakeTransport($this->infoAndPong());

        $payload = str_repeat('x', 300);

        // Model the responder under the muxed request inbox (#118): the request PUB carries its
        // reply-to ("<muxBase>.<token>") as the 3rd header token, and every reply is delivered on the
        // one long-lived wildcard subscription "<muxBase>.*" (sid 1). The instant the request PUB hits
        // the wire, echo the reply on that captured reply-to, split into one-byte chunks so every read
        // but the last is a partial (byte-consuming, frame-incomplete) read.
        $transport->onWrite = static function (string $bytes) use ($payload): array {
            $head = strtok($bytes, "\r\n");
            if ($head === false || !str_starts_with($head, 'PUB request.subject ')) {
                return [];
            }

            $replyTo = explode(' ', $head)[2] ?? ''; // PUB <subject> <replyTo> <len>
            if ($replyTo === '') {
                return [];
            }

            // Routed by the suffix token to this request's waiter on the mux sid (1).
            $frame = 'MSG ' . $replyTo . ' 1 ' . strlen($payload) . "\r\n" . $payload . "\r\n";

            return str_split($frame); // one byte per chunk: ~347 partial reads before completion
        };

        $connection = $this->connect($transport);

        $startedNs = hrtime(true);
        // Outer bound: fail loudly rather than hang if a regression parks the caller.
        $messages = await(
            [async(static fn (): array => $connection->requestMany('request.subject', 'ping', null, 1, 60)->await())],
            new TimeoutCancellation(5.0),
        )[0];
        $elapsedSeconds = (hrtime(true) - $startedNs) / 1e9;

        // kills LogicalNot @ line 1863: the reply is collected only when partial reads do NOT idle-sleep.
        self::assertCount(
            1,
            $messages,
            'the chunked reply must be drained inside the request budget, not idle-sleep 1 ms per partial chunk',
        );
        self::assertInstanceOf(NatsMessage::class, $messages[0]);
        self::assertSame($payload, $messages[0]->payload, 'the multi-chunk reply must reassemble byte-for-byte');
        self::assertLessThan(2.0, $elapsedSeconds, 'the request must terminate promptly, not hang');
    }
}
