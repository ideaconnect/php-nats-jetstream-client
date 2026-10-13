<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\IncomingChunkResult;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\NatsException;
use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Tests\Support\OwnsTestResources;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Transport\TransportInterface;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\async;
use function Amp\delay;

/**
 * A chunk a custom transport hands over late - after the connection the read started on was replaced, or closed - is
 * parsed with that connection's parser and dispatched as that connection's, the #182 rule for the old server's frames
 * then applying to all of it (#205). LateReadHoldingTransport wraps the scripted ReconnectingTransport and, when armed,
 * holds the chunk a read took until the test releases it; meanwhile a flush()'s failed PING write makes the recovery
 * reconnect (a legal TransportInterface implementation, if an unusual one; the built-in transports end a read their
 * close() interrupts). Each case runs through four readers: your readIncoming(), an operation's
 * readIncomingForOperation() with its own wake-up, and the heartbeat's consumeHeartbeatResponse() with and without its
 * tick's generation. Up to 2.26.5 the late bytes went into the new connection's parser and were dispatched as its own.
 */
final class LateTransportReadTest extends TestCase
{
    use OwnsTestResources;

    protected function tearDown(): void
    {
        $this->releaseOwnedResources();
    }

    /** @return iterable<string, array{string}> */
    public static function paths(): iterable
    {
        foreach (['public', 'operation', 'heartbeat', 'heartbeat without generation'] as $path) {
            yield $path => [$path];
        }
    }

    /** @return iterable<string, array{string, bool}> */
    public static function muxPaths(): iterable
    {
        foreach (self::paths() as $path => [$reader]) {
            yield $path . ', PONG' => [$reader, false];
            yield $path . ', reply' => [$reader, true];
        }
    }

    /**
     * [PING | half an MSG] read on the first connection, handed over once the second is open: no PONG on the new
     * connection for the old PING, and its parser is untouched - the new server's own PING is parsed as a frame and
     * answered. Before: the old PING answered there, and the half MSG swallowed the new server's PING as payload.
     */
    #[DataProvider('paths')]
    public function testLatePingAndPartialFrameLeaveReplacementParserClean(string $path): void
    {
        [$connection, $transport] = $this->fixture();
        $read = $this->holdRead($connection, $transport, $path, "PING\r\nMSG updates 9 10\r\nhal");
        $this->replaceConnection($connection, $transport);
        $transport->releaseRead();
        $this->finishRead($read);

        // Observe the replacement's own PING too, before asserting the stale-PING result.
        $transport->inner->pushFrame("PING\r\n");
        $frames = $connection->processIncoming()->await(new TimeoutCancellation(2));
        self::assertSame(1, $frames, 'replacement PING must be parsed as a frame');
        self::assertSame(['PONG'], $transport->inner->controlLinesStartingWith('PONG', 1), 'only the replacement PING gets a PONG');
    }

    /**
     * The late chunk finishes an MSG whose first bytes the old parser had buffered: "hello" is delivered to the
     * subscription the reconnect replayed, and a message on the new connection afterwards too.
     */
    #[DataProvider('paths')]
    public function testLateChunkFinishesTheOldParsersPartialMessage(string $path): void
    {
        [$connection, $transport] = $this->fixture();
        /** @var \ArrayObject<int, string> $received */
        $received = new \ArrayObject();
        $sid = $connection->subscribe('updates', static function (NatsMessage $message) use ($received): void {
            $received[] = $message->payload;
        })->await();
        $transport->inner->pushFrame("MSG updates {$sid} 5\r\nhe");
        self::assertSame(0, $connection->processIncoming()->await());
        $read = $this->holdRead($connection, $transport, $path, "llo\r\nMSG updates {$sid} 10\r\nhal");
        $this->replaceConnection($connection, $transport);
        $transport->releaseRead();
        $this->finishRead($read);

        self::assertSame(['hello'], $received->getArrayCopy(), 'complete old message must survive with its original parser');
        $transport->inner->pushFrame(ReconnectingTransport::msgFrame('updates', $sid, 'new'));
        self::assertSame(1, $connection->processIncoming()->await());
        self::assertSame(['hello', 'new'], $received->getArrayCopy());
    }

    /**
     * A late PONG, or a late reply on the shared inbox, does not confirm the inbox the new connection subscribed: the
     * new server's maximum-subscriptions -ERR still drops it, and the next request subscribes it again.
     */
    #[DataProvider('muxPaths')]
    public function testLatePongOrReplyDoesNotConfirmTheReplacementInbox(string $path, bool $reply): void
    {
        [$connection, $transport] = $this->fixture();
        $transport->inner->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $replyTo === null ? [] : $transport->inner->replyFrame($replyTo, 'ok');
        self::assertSame('ok', $connection->request('svc.echo', 'first', 2_000)->await()->payload);
        [$subscription] = $transport->inner->controlLinesStartingWith('SUB _INBOX.', 0);
        [, $inbox, $sid] = explode(' ', $subscription);
        $chunk = $reply
            ? ReconnectingTransport::msgFrame(substr($inbox, 0, -1) . 'late', (int) $sid, 'late')
            : "PONG\r\n";
        $read = $this->holdRead($connection, $transport, $path, $chunk);
        $transport->inner->answerPings = false;
        $this->replaceConnection($connection, $transport);
        $transport->releaseRead();
        $this->finishRead($read);

        $transport->inner->pushFrame("-ERR 'maximum subscriptions exceeded'\r\nPONG\r\n");
        try {
            $connection->processIncoming()->await();
            self::fail('the replacement server must reject its unconfirmed inbox');
        } catch (ConnectionException $error) {
            self::assertStringContainsString('maximum subscriptions exceeded', $error->getMessage());
        }
        $transport->inner->answerPings = true;
        self::assertSame('ok', $connection->request('svc.echo', 'second', 2_000)->await()->payload);
        self::assertCount(2, $transport->inner->controlLinesStartingWith('SUB _INBOX.', 1), 'stale PONG/reply must not confirm the replacement inbox');
    }

    /**
     * A late INFO leaves the new server's info alone, and a late -ERR is reported as the old server's error frame,
     * the connection staying Open.
     */
    #[DataProvider('paths')]
    public function testLateInfoAndErrorAreClassifiedAsTheOldServers(string $path): void
    {
        /** @var \ArrayObject<int, \Throwable> $errors */
        $errors = new \ArrayObject();
        [$connection, $transport] = $this->fixture(static function (\Throwable $error) use ($errors): void {
            $errors[] = $error;
        });
        $read = $this->holdRead($connection, $transport, $path, 'INFO {"server_id":"old","max_payload":1048576}' . "\r\n-ERR 'maximum subscriptions exceeded'\r\n");
        $this->replaceConnection($connection, $transport);
        $transport->releaseRead();
        $this->finishRead($read, 2);

        self::assertSame('S1', $connection->serverInfo()?->serverId, 'stale INFO must leave replacement server info alone');
        self::assertCount(1, $errors);
        $reported = $errors[0];
        self::assertInstanceOf(NatsException::class, $reported);
        self::assertSame("The server the connection left sent an error frame: 'maximum subscriptions exceeded'", $reported->getMessage());
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(2, $transport->inner->connectCalls);
    }

    /**
     * A late chunk ending in corrupt bytes: the HMSG ahead of them is delivered with its headers, its PING, PONG and
     * INFO have no effect on the new connection, its fatal -ERR is reported as the old server's and the parse failure
     * once, and no further recovery starts (two dials in all).
     */
    #[DataProvider('paths')]
    public function testLateCorruptChunkPreservesHeaderMessagesAndIgnoresOldControls(string $path): void
    {
        /** @var \ArrayObject<int, \Throwable> $errors */
        $errors = new \ArrayObject();
        [$connection, $transport] = $this->fixture(static function (\Throwable $error) use ($errors): void {
            $errors[] = $error;
        });
        /** @var \ArrayObject<int, array{string, ?string}> $received */
        $received = new \ArrayObject();
        $sid = $connection->subscribe('updates', static function (NatsMessage $message) use ($received): void {
            $received[] = [$message->payload, $message->rawHeaders];
        })->await();
        $headers = "NATS/1.0\r\nX-Test: retained\r\n\r\n";
        $chunk = ReconnectingTransport::hmsgFrame('updates', $sid, $headers, 'kept')
            . "PING\r\nPONG\r\n"
            . 'INFO {"server_id":"old","max_payload":1048576}' . "\r\n"
            . "-ERR 'Stale Connection'\r\nINVALID\r\n";
        $read = $this->holdRead($connection, $transport, $path, $chunk);
        $this->replaceConnection($connection, $transport);
        $transport->releaseRead();
        $this->finishRead($read, 5);

        self::assertSame([['kept', $headers]], $received->getArrayCopy(), 'HMSG ahead of corrupt bytes must survive with its headers');
        self::assertSame('S1', $connection->serverInfo()?->serverId, 'recovered stale INFO must leave replacement server info alone');
        self::assertCount(2, $errors, 'report both the old server error and the parse failure exactly once');
        $serverError = $errors[0];
        self::assertInstanceOf(NatsException::class, $serverError);
        self::assertSame("The server the connection left sent an error frame: 'Stale Connection'", $serverError->getMessage());
        self::assertInstanceOf(ProtocolException::class, $errors[1]);
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(2, $transport->inner->connectCalls, 'old corruption and fatal -ERR must not start another recovery');
        $transport->inner->pushFrame("PING\r\n");
        self::assertSame(1, $connection->processIncoming()->await());
        self::assertSame(['PONG'], $transport->inner->controlLinesStartingWith('PONG', 1), 'only the replacement PING gets a PONG');
    }

    /**
     * A disconnect() alone - which resets the parser but does not move the generation - keeps the late half MSG off
     * the reset parser: the close count captured with the read tells.
     */
    #[DataProvider('paths')]
    public function testTerminalCloseAloneKeepsLateBytesOffTheResetParser(string $path): void
    {
        [$connection, $transport] = $this->fixture();
        $generation = $this->generation($connection);
        $read = $this->holdRead($connection, $transport, $path, "PING\r\nMSG updates 9 10\r\nhal");
        $connection->disconnect()->await();
        self::assertSame($generation, $this->generation($connection), 'terminal close alone does not advance the generation');
        $parser = (new \ReflectionProperty(NatsConnection::class, 'parser'))->getValue($connection);
        $transport->releaseRead();
        $this->finishRead($read);

        self::assertNull((new \ReflectionProperty($parser, 'pending'))->getValue($parser), 'late bytes must not arm the reset parsers payload state');
        self::assertSame('', (new \ReflectionProperty($parser, 'buffer'))->getValue($parser));
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertCount(1, $transport->inner->connectCalls);
    }

    /**
     * Guard: a read handed over late with no reconnect in between is the connection's own: it finishes a split HMSG
     * and its PING is answered on that connection.
     */
    #[DataProvider('paths')]
    public function testSameConnectionStillHandlesControlsAndSplitHeaderMessages(string $path): void
    {
        [$connection, $transport] = $this->fixture();
        /** @var \ArrayObject<int, array{string, ?string}> $received */
        $received = new \ArrayObject();
        $sid = $connection->subscribe('updates', static function (NatsMessage $message) use ($received): void {
            $received[] = [$message->payload, $message->rawHeaders];
        })->await();
        $frame = ReconnectingTransport::hmsgFrame('updates', $sid, "NATS/1.0\r\nX-Test: kept\r\n\r\n", 'hello');
        $transport->inner->pushFrame(substr($frame, 0, -4));
        self::assertSame(0, $connection->processIncoming()->await());
        $read = $this->holdRead($connection, $transport, $path, substr($frame, -4) . "PING\r\nPONG\r\n");
        $transport->releaseRead();
        $this->finishRead($read, 3);

        self::assertSame([['hello', "NATS/1.0\r\nX-Test: kept\r\n\r\n"]], $received->getArrayCopy());
        self::assertSame(['PONG'], $transport->inner->controlLinesStartingWith('PONG', 0));
        self::assertCount(1, $transport->inner->connectCalls);
    }

    /** @return iterable<string, array{string}> */
    public static function ordinaryPaths(): iterable
    {
        yield 'public' => ['public'];
        yield 'operation' => ['operation'];
    }

    /**
     * A corrupt late chunk does not join the new connection's own recovery, whose dial is held: the old read returns
     * (its complete message delivered, the parse failure reported) while that recovery still waits.
     */
    #[DataProvider('ordinaryPaths')]
    public function testCorruptOldChunkDoesNotJoinAnUnrelatedReplacementRecovery(string $path): void
    {
        $parseReported = new DeferredFuture();
        [$connection, $transport] = $this->fixture(static function (\Throwable $error) use ($parseReported): void {
            if ($error instanceof ProtocolException && !$parseReported->isComplete()) {
                $parseReported->complete();
            }
        });
        /** @var \ArrayObject<int, string> $received */
        $received = new \ArrayObject();
        $sid = $connection->subscribe('updates', static function (NatsMessage $message) use ($received): void {
            $received[] = $message->payload;
        })->await();
        $read = $this->holdRead($connection, $transport, $path, ReconnectingTransport::msgFrame('updates', $sid, 'kept') . "INVALID\r\n");
        $this->replaceConnection($connection, $transport);
        $transport->inner->holdNextDial();
        $this->resources()->onRelease($transport->inner->releaseDial(...), 'unrelated recovery dial');
        $transport->inner->failNextWriteContaining("PING\r\n");
        $replacementFlush = $this->resources()->ownOperation($connection->flush(), label: 'replacement failing flush');
        $this->waitUntil(static fn(): bool => count($transport->inner->connectCalls) === 3);
        $transport->releaseRead();
        $parseReported->getFuture()->await(new TimeoutCancellation(2));
        delay(0);
        self::assertTrue($read->isComplete(), 'stale parse failure must not join unrelated replacement recovery');
        $this->finishRead($read);
        self::assertSame(['kept'], $received->getArrayCopy());
        self::assertSame(ConnectionState::Connecting, $connection->state());
        $transport->inner->releaseDial();
        $this->expectFailedFlush($replacementFlush);
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(3, $transport->inner->connectCalls);
    }

    /** A heartbeat tick whose connection was replaced while its PING was written does not read the new socket. */
    public function testAnObsoleteHeartbeatGenerationDoesNotStartReadingTheReplacement(): void
    {
        [$connection, $transport] = $this->fixture();
        $generation = $this->generation($connection);
        $this->replaceConnection($connection, $transport);
        $transport->inner->pushFrame("PING\r\n");
        $readsBefore = $transport->readStarts;
        (new \ReflectionMethod(NatsConnection::class, 'consumeHeartbeatResponse'))->invoke($connection, $generation);
        self::assertSame($readsBefore, $transport->readStarts, 'obsolete heartbeat must not read the replacement socket');
        self::assertSame(1, $connection->processIncoming()->await());
        self::assertSame(['PONG'], $transport->inner->controlLinesStartingWith('PONG', 1));
    }

    /** @return array{NatsConnection, LateReadHoldingTransport} */
    private function fixture(?\Closure $errorListener = null): array
    {
        $transport = new LateReadHoldingTransport(new ReconnectingTransport());
        $connection = $this->own(new NatsConnection(new NatsOptions(
            servers: ['nats://one:4222'],
            requestTimeoutMs: 2_000,
            reconnectDelayMs: 0,
            reconnectMaxDelayMs: 0,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 3_600,
            errorListener: $errorListener,
        ), $transport));
        $connection->connect()->await();

        return [$connection, $transport];
    }

    /** @return Future<mixed> */
    private function holdRead(NatsConnection $connection, LateReadHoldingTransport $transport, string $path, string $chunk): Future
    {
        $ownSid = $path === 'operation'
            ? $connection->subscribe('operation.wake', static function (NatsMessage $message): void {})->await()
            : null;
        $transport->armRead();
        $this->resources()->onRelease($transport->releaseRead(...), 'held read completion');
        $this->resources()->expectIdle(static fn(): bool => $transport->heldReads === 0, 'held read completion');
        $transport->inner->pushFrame($chunk);
        $stop = new DeferredCancellation();
        $read = match ($path) {
            'public' => $connection->readIncoming($stop->getCancellation()),
            'operation' => $connection->readIncomingForOperation($stop->getCancellation(), $ownSid),
            default => async(function () use ($connection, $path): void {
                (new \ReflectionMethod(NatsConnection::class, 'consumeHeartbeatResponse'))->invoke(
                    $connection,
                    $path === 'heartbeat' ? $this->generation($connection) : null,
                );
            }),
        };
        $this->resources()->ownOperation($read, $stop->cancel(...), 'held reader');
        $transport->captured?->getFuture()->await(new TimeoutCancellation(2));

        return $read;
    }

    /** @param Future<mixed> $read */
    private function finishRead(Future $read, int $expectedFrames = 1): void
    {
        $error = null;
        $result = null;
        try {
            $result = $read->await(new TimeoutCancellation(2));
        } catch (\Throwable $failure) {
            $error = $failure;
        }
        self::assertNull($error, 'late read must finish without applying an old control-frame failure');
        if ($result instanceof IncomingChunkResult) {
            self::assertTrue($result->consumedBytes, 'a complete late chunk still counts as wire progress');
            self::assertSame($expectedFrames, $result->frames, 'the read counts frames parsed on its original connection');
        }
    }

    private function replaceConnection(NatsConnection $connection, LateReadHoldingTransport $transport): void
    {
        $transport->inner->failNextWriteContaining("PING\r\n");
        $this->expectFailedFlush($connection->flush());
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(2, $transport->inner->connectCalls);
    }

    /** @param Future<void> $flush */
    private function expectFailedFlush(Future $flush): void
    {
        try {
            $flush->await(new TimeoutCancellation(2));
            self::fail('the PING write must fail');
        } catch (ConnectionException $error) {
            self::assertSame('Connection lost before the server answered the PING', $error->getMessage());
        }
    }

    private function generation(NatsConnection $connection): int
    {
        return (new \ReflectionProperty(NatsConnection::class, 'connectionGeneration'))->getValue($connection);
    }

    /** @param \Closure(): bool $condition */
    private function waitUntil(\Closure $condition): void
    {
        $deadline = hrtime(true) + 2_000_000_000;
        while (!$condition()) {
            if (hrtime(true) > $deadline) {
                self::fail('fixture condition must be reached');
            }
            delay(0.001);
        }
    }
}

/** Deliberately lets one old-session read return late; all writes retain the scripted server's FIFO order. */
final class LateReadHoldingTransport implements TransportInterface
{
    /** @var DeferredFuture<null>|null */
    public ?DeferredFuture $captured = null;
    /** @var DeferredFuture<null>|null */
    private ?DeferredFuture $gate = null;
    private bool $armed = false;
    private int $session = 0;
    /** @var list<string> */
    private array $restored = [];
    public int $heldReads = 0;
    public int $readStarts = 0;

    public function __construct(public readonly ReconnectingTransport $inner) {}

    public function armRead(): void
    {
        $this->armed = true;
        $this->gate = new DeferredFuture();
        $this->captured = new DeferredFuture();
    }

    public function releaseRead(): void
    {
        if ($this->gate !== null && !$this->gate->isComplete()) {
            $this->gate->complete();
        }
    }

    public function connect(string $dsn, int $timeoutMs): Future
    {
        $this->session++;
        $this->restored = [];

        return $this->inner->connect($dsn, $timeoutMs);
    }

    public function upgradeTls(): Future
    {
        return $this->inner->upgradeTls();
    }

    public function write(string $bytes): Future
    {
        return $this->inner->write($bytes);
    }

    public function readLine(?Cancellation $cancellation = null): Future
    {
        $this->readStarts++;
        if (!$this->armed) {
            if ($this->restored !== []) {
                return Future::complete(array_shift($this->restored));
            }

            return $this->inner->readLine($cancellation);
        }
        $this->armed = false;
        $read = $this->inner->readLine($cancellation);
        $gate = $this->gate;
        $captured = $this->captured;
        $session = $this->session;

        return async(function () use ($read, $gate, $captured, $session, $cancellation): string {
            $chunk = $read->await();
            $this->heldReads++;
            $captured?->complete();
            $keepAlive = EventLoop::delay(10, static function (): void {});
            try {
                $gate?->getFuture()->await($cancellation);

                return $chunk;
            } catch (CancelledException $cancelled) {
                // Honour the existing cancellation contract: unreturned bytes remain readable on their session.
                if ($session === $this->session) {
                    array_unshift($this->restored, $chunk);
                }

                throw $cancelled;
            } finally {
                $this->heldReads--;
                EventLoop::cancel($keepAlive);
            }
        });
    }

    public function close(): Future
    {
        $this->session++;
        $this->restored = [];

        return $this->inner->close();
    }
}
