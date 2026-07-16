<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use Amp\TimeoutCancellation;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Core\SubscriptionQueue;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\Future\await;

/**
 * Kills the surviving Infection mutant on {@see SubscriptionQueue::fetchAll()}'s
 * `$hasTimeout = $this->timeout > 0` decision (line 248).
 *
 * `$hasTimeout` controls the read loop's termination policy: when FALSE the loop breaks on the very
 * first `frames === 0` read (a single best-effort cycle, line 261); when TRUE it keeps looping,
 * waiting for the full window. The GreaterThan mutant (`> 0` -> `>= 0`) flips the classification for a
 * queue with `timeout === 0`, turning the intended single-cycle best-effort read into a full-drain
 * loop.
 */
final class SubscriptionQueue_2MutationTest extends TestCase
{
    private function makeConnectedClient(FakeTransport $transport): NatsClient
    {
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        return $client;
    }

    /** @return list<string> */
    private function infoAndPong(): array
    {
        return [
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ];
    }

    /**
     * kills GreaterThan @ line 248 (`$hasTimeout = $this->timeout > 0` -> `$this->timeout >= 0`).
     *
     * With `timeout === 0` the queue must treat itself as having NO timeout: `$hasTimeout` is false, so
     * fetchAll() does a SINGLE best-effort read and breaks the moment a read yields zero complete
     * frames (line 261), returning whatever is already buffered.
     *
     * The transport delivers ONE message split across two chunks: the first chunk consumes bytes but
     * completes no frame (frames = 0, consumedBytes = true); the second chunk would complete it.
     *
     * REAL code (`> 0`, hasTimeout = false): the first read reports frames = 0 with an empty buffer, so
     * `!$hasTimeout` fires and the loop breaks BEFORE the completing chunk is ever read - the split
     * frame never assembles and fetchAll() returns [].
     *
     * MUTANT (`>= 0`, hasTimeout = true): `!$hasTimeout` is false, so the loop does not break; it reads
     * on, consumes the completing chunk, assembles the message, and returns [message]. assertSame([],
     * ...) therefore fails on the mutant.
     */
    public function testFetchAllTimeoutZeroBreaksOnFirstEmptyFrameNotFullDrain(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = $this->makeConnectedClient($transport);

        $queue = $client->subscribeQueue('events')->await();
        self::assertInstanceOf(SubscriptionQueue::class, $queue);
        // timeout stays 0 -> the boundary the mutant flips: real hasTimeout=false, mutant hasTimeout=true.

        $payload = 'abc';
        $frame = 'MSG events ' . $queue->sid . ' ' . strlen($payload) . "\r\n" . $payload . "\r\n";
        $split = (int) floor(strlen($frame) / 2);
        // Chunk 1: a partial frame - consumes bytes, completes 0 frames.
        $transport->pushReadChunk(substr($frame, 0, $split));
        // Chunk 2: completes the frame - only read if the loop keeps going past the first empty read.
        $transport->pushReadChunk(substr($frame, $split));

        // Outer bound: fail loudly rather than hang if a regression parks the caller.
        $result = await(
            [async(static fn (): array => $queue->fetchAll())],
            new TimeoutCancellation(5.0),
        )[0];

        // kills GreaterThan @ 248: single best-effort cycle breaks before the completing chunk, so the
        // split frame never assembles; the mutant keeps looping, assembles it, and returns [message].
        self::assertSame([], $result, 'timeout 0 must be a single best-effort cycle, not a full-drain loop');
    }
}
