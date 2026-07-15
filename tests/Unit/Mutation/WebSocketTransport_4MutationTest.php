<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use Amp\Socket\ResourceSocket;
use Amp\Socket\Socket;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Tests\Support\ScriptedChunkSocket;
use IDCT\NATS\Transport\WebSocketTransport;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\Socket\connect;
use function Amp\Socket\listen;

/**
 * Mutation kills for {@see WebSocketTransport::applyReadChunkSize()} (src line 179-185, #119).
 *
 * The guard is `if ($this->socket instanceof ResourceSocket && $chunkSize > 0)` then
 * `$this->socket->setChunkSize($chunkSize)`. Every surviving mutant either flips one operand, the
 * connective, or removes the call; each is pinned by an observable side effect of setChunkSize
 * (the cap on how many bytes a single read may return) or by whether the no-op guard fires at all.
 */
final class WebSocketTransport_4MutationTest extends TestCase
{
    /**
     * With a real ResourceSocket and a positive readChunkSizeBytes, applyReadChunkSize() MUST call
     * setChunkSize($chunkSize), which caps how many bytes a single read may return. The server writes
     * far more than the configured cap; the real code returns exactly the cap, while any mutant that
     * skips the call leaves Amp's 8192-byte default and returns every buffered byte.
     *
     * kills GreaterThanNegotiation @ 182  ($chunkSize > 0 -> <= 0: 16 <= 0 is false, guard skips call)
     * kills LogicalAndAllSubExprNegation @ 182  (!ResourceSocket && !(>0): false && false, guard skips)
     * kills LogicalAndNegation @ 182  (!(true && true) = false, guard skips call)
     * kills MethodCallRemoval @ 183  (call deleted -> default chunk size stays)
     */
    public function testAppliesConfiguredReadChunkSizeToResourceSocket(): void
    {
        $chunkSizeBytes = 16;

        $server = listen('tcp://127.0.0.1:0');
        $address = (string) $server->getAddress();
        $accept = async(static fn (): ?Socket => $server->accept());
        $client = connect('tcp://' . $address);
        $serverSocket = $accept->await();

        try {
            self::assertInstanceOf(Socket::class, $serverSocket);
            // Precondition: connect() yields a ResourceSocket so the guard's instanceof arm is true.
            self::assertInstanceOf(ResourceSocket::class, $client);

            $transport = new WebSocketTransport(new NatsOptions(readChunkSizeBytes: $chunkSizeBytes));
            (new \ReflectionProperty(WebSocketTransport::class, 'socket'))->setValue($transport, $client);

            (new \ReflectionMethod(WebSocketTransport::class, 'applyReadChunkSize'))->invoke($transport);

            // Far more than the cap, delivered atomically over loopback so >= 16 bytes are buffered.
            $serverSocket->write(str_repeat('x', 256));

            // Real code: setChunkSize(16) applied -> a single read returns exactly 16 bytes. Any mutant
            // that skips the call leaves the 8192 default -> read returns all 256 buffered bytes.
            $chunk = (string) $client->read(new TimeoutCancellation(3.0));
            self::assertSame($chunkSizeBytes, strlen($chunk));
        } finally {
            $client->close();
            $serverSocket->close();
            $server->close();
        }
    }

    /**
     * When the socket is NOT a ResourceSocket, applyReadChunkSize() MUST be a no-op: setChunkSize is
     * not on the Socket interface, so calling it would be an Error. ScriptedChunkSocket is a Socket
     * without setChunkSize; with a positive chunk size the real `&&` guard is `false && true` = false
     * (no call, no error). The `||` mutant makes it `false || true` = true and calls the missing
     * setChunkSize, raising an Error - so the successful, side-effect-free invocation kills it.
     *
     * kills LogicalAnd @ 182  (&& -> ||: false || true fires setChunkSize on a non-ResourceSocket)
     * kills LogicalAndNegation @ 182  (!(false && true) = true also fires it -> Error)
     */
    public function testDoesNotCallSetChunkSizeOnNonResourceSocket(): void
    {
        $socket = new ScriptedChunkSocket(['HELLO']);
        $transport = new WebSocketTransport(new NatsOptions()); // default readChunkSizeBytes = 131072 (> 0)
        (new \ReflectionProperty(WebSocketTransport::class, 'socket'))->setValue($transport, $socket);

        // Real code returns without touching the socket. A mutant that fires the guard calls the
        // undefined setChunkSize on the fake -> Error, failing this invocation.
        (new \ReflectionMethod(WebSocketTransport::class, 'applyReadChunkSize'))->invoke($transport);

        // Observable no-op: the untouched socket still hands out its scripted chunk unchanged.
        self::assertSame('HELLO', $socket->read());
    }
}
