<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use Amp\Socket\ResourceSocket;
use Amp\Socket\Socket;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Tests\Support\ScriptedChunkSocket;
use IDCT\NATS\Transport\AmpSocketTransport;

/**
 * Targeted tests that pin the read-chunk-size guard Infection mutated in {@see AmpSocketTransport}
 * (chunk 1, #119 read-path change). The guard under test lives at line 86:
 *
 *     if ($this->socket instanceof ResourceSocket && $chunkSize > 0) {
 *         $this->socket->setChunkSize($chunkSize);
 *     }
 *
 * The whole point of the guard is that setChunkSize() exists ONLY on ResourceSocket, so it must NOT
 * be called for any other Socket implementation. We drive the private applyReadChunkSize() directly
 * with a non-ResourceSocket socket injected via reflection - no Docker, no real NATS server, no
 * wall-clock timing.
 */
final class AmpSocketTransport_1MutationTest extends \PHPUnit\Framework\TestCase
{
    /**
     * The guard must AND the instanceof check with the size check: for a Socket that is NOT a
     * ResourceSocket, setChunkSize() must never be invoked (that method exists only on
     * ResourceSocket, so calling it would blow up).
     *
     * With the default readChunkSizeBytes (131072 > 0) and a non-ResourceSocket socket the two
     * operands diverge: `false && true`. The real `&&` short-circuits to false and skips the body;
     * the `||` mutant evaluates `false || true` => true, enters the body, and calls
     * $this->socket->setChunkSize() on a ScriptedChunkSocket that has no such method - a fatal
     * "call to undefined method" Error. So the real code runs clean and the mutant throws.
     */
    public function testChunkSizeNotAppliedToNonResourceSocket(): void
    {
        $transport = new AmpSocketTransport(new NatsOptions());

        // Precondition the whole kill relies on: the injected socket is a real Socket but NOT a
        // ResourceSocket, and it has no setChunkSize() method.
        // ScriptedChunkSocket is a plain Socket (NOT a ResourceSocket) with no setChunkSize() - the
        // precondition the kill below relies on (guaranteed by its type, so no runtime assert needed).
        $socket = new ScriptedChunkSocket([]);
        self::assertInstanceOf(Socket::class, $socket);

        $socketProperty = new \ReflectionProperty(AmpSocketTransport::class, 'socket');
        $socketProperty->setValue($transport, $socket);

        $apply = new \ReflectionMethod(AmpSocketTransport::class, 'applyReadChunkSize');

        // kills LogicalAnd @ line 86: real `&&` skips the body for a non-ResourceSocket (no throw);
        // the `||` mutant enters the body and calls the nonexistent setChunkSize() => Error.
        try {
            $apply->invoke($transport);
        } catch (\Throwable $e) {
            self::fail(
                'applyReadChunkSize() must be a no-op for a non-ResourceSocket socket, but it threw: '
                . $e::class . ': ' . $e->getMessage(),
            );
        }

        // Reaching here means the guard correctly short-circuited and left the socket untouched.
        self::addToAssertionCount(1);
    }
}
