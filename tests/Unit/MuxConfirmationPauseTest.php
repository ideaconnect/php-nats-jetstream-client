<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use Closure;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Tests\Support\OwnsTestResources;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\WatchedTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionProperty;
use Revolt\EventLoop;
use Revolt\EventLoop\CallbackType;
use Throwable;

use function Amp\async;
use function Amp\Future\awaitFirst;

/**
 * A request or requestMany() waiting for its replacement reply inbox to be confirmed checks, before each confirmation
 * read, that the inbox is still the one it waits for (#204). After the shared inbox was rejected once, a second request
 * installs a new one and waits behind an application read; `+OK` ends that read, so the request's own read returns
 * without bytes and it pauses for 1 ms. The test holds that real pause timer (disabled, then enabled again) while the
 * application's second read takes what the case sends. Up to 2.26.1 a drop met in that pause left the next read with a
 * forgotten fence and no wake-up: the request waited out its timeout, or a lame-duck failover inline.
 */
final class MuxConfirmationPauseTest extends TestCase
{
    use OwnsTestResources;

    private const LIMIT = "-ERR 'maximum subscriptions exceeded'\r\n";
    private const DROPPED = 'the server may have rejected the shared reply-inbox subscription';
    private const LAME_DUCK = 'INFO {"server_id":"S1","version":"2.12.0","max_payload":1048576,"ldm":true,"connect_urls":["10.0.0.2:4222"]}' . "\r\n";

    protected function tearDown(): void
    {
        $this->releaseOwnedResources();
    }

    /** @return iterable<string, array{string}> */
    public static function operations(): iterable
    {
        yield 'request' => ['request'];
        yield 'requestMany' => ['requestMany'];
    }

    /** @return iterable<string, array{string, string}> */
    public static function drops(): iterable
    {
        foreach (['request', 'requestMany'] as $operation) {
            foreach (['limit', 'limit-ldm', 'permissions'] as $shape) {
                yield "$operation/$shape" => [$operation, $shape];
            }
        }
    }

    /**
     * The other read takes the limit `-ERR` (also followed by a queued lame-duck INFO) or a permissions violation naming
     * the new inbox, then a PONG, during the pause. Resumed, the request fails with the dropped-inbox or the detailed
     * permissions error, without a previous exception, before any other socket read: one PUB in all, no failover dial.
     * After a limit rejection a retry still installs a third inbox and gets its reply.
     */
    #[DataProvider('drops')]
    public function testADropDuringThePauseFailsBeforeAnotherSocketRead(string $operation, string $shape): void
    {
        [$transport, $watched, $connection] = $this->prime();
        [$request, $reader, $pause] = $this->pauseConfirmation($transport, $watched, $connection, $operation);

        $frame = self::LIMIT;
        if ($shape === 'permissions') {
            $wildcard = explode(' ', $transport->controlLinesStartingWith('SUB _INBOX.')[1])[1];
            $frame = "-ERR 'Permissions Violation for Subscription to \"$wildcard\"'\r\n";
        }
        $transport->pushFrame($frame . "PONG\r\n");
        $reader->await(new TimeoutCancellation(2));
        self::assertNull((new ReflectionProperty(NatsConnection::class, 'muxFence'))->getValue($connection));
        self::assertFalse($request->isComplete(), 'the request is held in the real confirmation pause');

        if ($shape === 'limit-ldm') {
            $transport->holdNextDial();
            $this->resources()->onRelease($transport->releaseDial(...), 'held lame-duck dial');
            $transport->pushFrame(self::LAME_DUCK);
        }
        $error = $this->resumeWithoutReading($request, $watched, $pause);

        self::assertInstanceOf(ConnectionException::class, $error);
        self::assertStringContainsString($shape === 'permissions' ? 'was rejected by the server (permissions violation)' : self::DROPPED, $error->getMessage());
        self::assertNull($error->getPrevious(), 'the pre-read guard throws the rejection directly, outside the read catch');
        self::assertCount(1, $transport->controlLinesStartingWith('PUB svc.echo'), 'the second request never reached the responder');
        self::assertCount(1, $transport->connectCalls, 'the failed request does not read the queued lame-duck INFO');

        if ($shape === 'limit') {
            // The transient rejection still permits a new inbox and a successful retry.
            $transport->answerPings = true;
            $transport->responder = static fn(string $subject, ?string $replyTo): array => $replyTo === null ? [] : $transport->replyFrame($replyTo, 'ok');
            self::assertSame('ok', $connection->request('svc.echo', 'retry', 1_000)->await()->payload);
            self::assertCount(3, $transport->controlLinesStartingWith('SUB _INBOX.'));
        }
    }

    /** @return iterable<string, array{string, bool}> */
    public static function replacements(): iterable
    {
        foreach (['request', 'requestMany'] as $operation) {
            yield "$operation/unconfirmed replacement" => [$operation, false];
            yield "$operation/confirmed replacement" => [$operation, true];
        }
    }

    /**
     * After the drop in the pause a third request installs a replacement inbox, left unconfirmed or confirmed. The
     * original request still fails with the dropped-inbox error and is not sent on the replacement; the replacement
     * request gets its reply. Without the check before the read the unconfirmed row waited for the replacement's PONG.
     */
    #[DataProvider('replacements')]
    public function testAReplacementInboxCannotAdoptTheRequestWhoseInboxWasDropped(string $operation, bool $confirmed): void
    {
        [$transport, $watched, $connection] = $this->prime();
        [$request, $reader, $pause] = $this->pauseConfirmation($transport, $watched, $connection, $operation);
        $transport->pushFrame(self::LIMIT . "PONG\r\n");
        $reader->await(new TimeoutCancellation(2));

        $transport->responder = static fn(string $subject, ?string $replyTo): array => $replyTo === null ? [] : $transport->replyFrame($replyTo, 'ok');
        $replacement = $this->request($connection, 'request');
        $this->until(static fn(): bool => count($transport->controlLinesStartingWith('SUB _INBOX.')) === 3 && $watched->readsUnderWay === 1);
        self::assertNotNull((new ReflectionProperty(NatsConnection::class, 'muxFence'))->getValue($connection), 'a different inbox now has a live confirmation fence');

        if ($confirmed) {
            $transport->pushFrame("PONG\r\n");
            $replacement->await(new TimeoutCancellation(2));
        }

        EventLoop::enable($pause);
        try {
            $error = self::outcome($request)->await(new TimeoutCancellation(1));
        } catch (CancelledException) {
            self::fail('the original request must fail before the replacement inbox is confirmed');
        }
        self::assertInstanceOf(ConnectionException::class, $error);
        self::assertStringContainsString(self::DROPPED, $error->getMessage());
        self::assertNull($error->getPrevious());
        self::assertCount($confirmed ? 2 : 1, $transport->controlLinesStartingWith('PUB svc.echo'), 'the original request was not sent on the replacement inbox');

        if (!$confirmed) {
            self::assertFalse($replacement->isComplete(), 'the replacement request still waits for its own confirmation');
            $transport->pushFrame("PONG\r\n");
        }
        $reply = $replacement->await(new TimeoutCancellation(2));
        self::assertInstanceOf(NatsMessage::class, $reply);
        self::assertSame('ok', $reply->payload);
        self::assertCount(2, $transport->controlLinesStartingWith('PUB svc.echo'));
    }

    /**
     * Guard: the limit `-ERR` in the read the request waits behind fails it at once, as before, by the check after that
     * read and not after another pause: every pause timer started meanwhile is held, and the request still ends. One PUB
     * in all.
     */
    #[DataProvider('operations')]
    public function testADropInTheReadTheRequestWaitsBehindStillFailsWithoutAPublish(string $operation): void
    {
        [$transport, $watched, $connection] = $this->prime();
        [$request, $reader] = $this->startConfirmationBehindTwoReads($transport, $watched, $connection, $operation);
        $before = array_fill_keys(EventLoop::getIdentifiers(), true);
        /** @var \ArrayObject<int, string> $held */
        $held = new \ArrayObject();
        $this->resources()->onRelease(static function () use ($held): void {
            foreach ($held as $id) {
                if (in_array($id, EventLoop::getIdentifiers(), true)) {
                    EventLoop::enable($id);
                }
            }
        }, 'held pause timers');
        $ended = self::outcome($request);
        $transport->pushFrame(self::LIMIT . "PONG\r\n");
        for ($index = 0; $index < 500 && !$ended->isComplete(); $index++) {
            foreach (EventLoop::getIdentifiers() as $id) {
                if (!isset($before[$id]) && !in_array($id, $held->getArrayCopy(), true) && EventLoop::getType($id) === CallbackType::Delay) {
                    EventLoop::disable($id);
                    $held[] = $id;
                }
            }
            self::checkpoint();
        }
        self::assertTrue($ended->isComplete(), 'the request fails after the read that met the -ERR, without another pause');
        $error = $ended->await();
        self::assertInstanceOf(ConnectionException::class, $error);
        self::assertStringContainsString(self::DROPPED, $error->getMessage());
        self::assertCount(1, $transport->controlLinesStartingWith('PUB svc.echo'));
        // Let the application's second read finish as well.
        $transport->pushFrame("+OK\r\n");
        $reader->await(new TimeoutCancellation(2));
    }

    /** Guard: the PONG that confirms the inbox, taken during the pause, lets the request be sent and answered. */
    #[DataProvider('operations')]
    public function testAConfirmationDuringThePauseAllowsTheRequestToBeSent(string $operation): void
    {
        [$transport, $watched, $connection] = $this->prime();
        [$request, $reader, $pause] = $this->pauseConfirmation($transport, $watched, $connection, $operation);
        $transport->responder = static fn(string $subject, ?string $replyTo): array => $replyTo === null ? [] : $transport->replyFrame($replyTo, 'ok');
        $transport->pushFrame("PONG\r\n");
        $reader->await(new TimeoutCancellation(2));
        EventLoop::enable($pause);
        $result = $request->await(new TimeoutCancellation(2));
        self::assertSame('ok', is_array($result) ? $result[0]->payload : $result->payload);
        self::assertCount(2, $transport->controlLinesStartingWith('PUB svc.echo'));
        self::assertCount(2, $transport->controlLinesStartingWith('SUB _INBOX.'));
    }

    /** Guard: a disconnect() during the pause still fails the request with "Connection is not open", not sent. */
    #[DataProvider('operations')]
    public function testATerminalCloseDuringThePauseKeepsItsConnectionError(string $operation): void
    {
        [$transport, $watched, $connection] = $this->prime();
        [$request, , $pause] = $this->pauseConfirmation($transport, $watched, $connection, $operation);
        $connection->disconnect()->await();
        if (in_array($pause, EventLoop::getIdentifiers(), true)) {
            EventLoop::enable($pause);
        }
        $error = self::outcome($request)->await(new TimeoutCancellation(2));
        self::assertInstanceOf(ConnectionException::class, $error);
        self::assertSame('Connection is not open', $error->getMessage());
        self::assertCount(1, $transport->controlLinesStartingWith('PUB svc.echo'));
    }

    /** @return array{ReconnectingTransport, WatchedTransport, NatsConnection} */
    private function prime(): array
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $connection = $this->own(new NatsConnection(new NatsOptions(connectTimeoutMs: 5_000, pingIntervalSeconds: 0), $watched));
        $connection->connect()->await();
        $transport->answerPings = false;
        $first = $this->request($connection, 'request');
        $this->until(static fn(): bool => count($transport->controlLinesStartingWith('PUB svc.echo')) === 1);
        $transport->pushFrame(self::LIMIT . "PONG\r\n");
        $error = self::outcome($first)->await(new TimeoutCancellation(2));
        self::assertInstanceOf(ConnectionException::class, $error);
        self::assertStringContainsString('maximum subscriptions exceeded', $error->getMessage());

        return [$transport, $watched, $connection];
    }

    /** @return array{Future<NatsMessage|list<NatsMessage>>, Future<void>} */
    private function startConfirmationBehindTwoReads(ReconnectingTransport $transport, WatchedTransport $watched, NatsConnection $connection, string $operation): array
    {
        $stop = new DeferredCancellation();
        $reader = $this->resources()->ownOperation(async(static function () use ($connection, $stop): void {
            for ($index = 0; $index < 2; $index++) {
                try {
                    $connection->processIncoming($stop->getCancellation())->await();
                } catch (ConnectionException) {
                    // The subscription-limit frame fails the application's read while keeping the connection.
                }
            }
        }), $stop->cancel(...), 'two application reads');
        $this->until(static fn(): bool => $watched->readsUnderWay === 1);
        $request = $this->request($connection, $operation);
        $this->until(static fn(): bool => count($transport->controlLinesStartingWith('SUB _INBOX.')) === 2);
        for ($index = 0; $index < 12; $index++) {
            self::checkpoint();
        }
        self::assertFalse($request->isComplete());

        return [$request, $reader];
    }

    /** @return array{Future<NatsMessage|list<NatsMessage>>, Future<void>, string} */
    private function pauseConfirmation(ReconnectingTransport $transport, WatchedTransport $watched, NatsConnection $connection, string $operation): array
    {
        [$request, $reader] = $this->startConfirmationBehindTwoReads($transport, $watched, $connection, $operation);
        // Snapshot existing callbacks before +OK. An idle confirmation read creates the next delay.
        // Disable that particular timer to hold the actual 1 ms pause until the other read has dropped the mux.
        // This widens the scheduling window without patching the library or changing read/write order.
        $before = array_fill_keys(EventLoop::getIdentifiers(), true);
        $transport->pushFrame("+OK\r\n");
        $pause = null;
        for ($index = 0; $index < 500 && $pause === null; $index++) {
            foreach (EventLoop::getIdentifiers() as $id) {
                if (!isset($before[$id]) && EventLoop::getType($id) === CallbackType::Delay) {
                    $pause = $id;
                    EventLoop::disable($pause);
                    break;
                }
            }
            if ($pause === null) {
                self::checkpoint();
            }
        }
        self::assertNotNull($pause, 'the confirmation must actually reach its idle-read pause');
        $this->resources()->onRelease(static function () use ($pause): void {
            if (in_array($pause, EventLoop::getIdentifiers(), true)) {
                EventLoop::enable($pause);
            }
        }, 'held confirmation pause');
        $this->until(static fn(): bool => $watched->readsUnderWay === 1);

        return [$request, $reader, $pause];
    }

    /** @param Future<NatsMessage|list<NatsMessage>> $request */
    private function resumeWithoutReading(Future $request, WatchedTransport $watched, string $pause): ?Throwable
    {
        /** @var DeferredFuture<string> $read */
        $read = new DeferredFuture();
        $watched->onRead = static function () use ($read): void {
            if (!$read->isComplete()) {
                $read->complete('read');
            }
        };
        $this->resources()->onStop(static function () use ($watched): void {
            $watched->onRead = null;
        }, 'read observer');
        $ended = self::outcome($request);
        EventLoop::enable($pause);
        $event = awaitFirst([$ended->map(static fn(): string => 'ended'), $read->getFuture()], new TimeoutCancellation(2));
        $watched->onRead = null;
        self::assertSame('ended', $event, 'a dropped inbox must fail before another socket read');

        return $ended->await();
    }

    /** @return Future<NatsMessage|list<NatsMessage>> */
    private function request(NatsConnection $connection, string $operation): Future
    {
        $cancel = new DeferredCancellation();
        $request = $operation === 'request'
            ? $connection->request('svc.echo', 'payload', 3_000, $cancel->getCancellation())
            : $connection->requestMany('svc.echo', 'payload', null, 1, 3_000, cancellation: $cancel->getCancellation());

        return $this->resources()->ownOperation($request, $cancel->cancel(...), $operation);
    }

    /** @param Future<NatsMessage|list<NatsMessage>> $request
     *  @return Future<?Throwable>
     */
    private static function outcome(Future $request): Future
    {
        return $request->map(static fn(): ?Throwable => null)->catch(static fn(Throwable $error): Throwable => $error);
    }

    /** @param Closure(): bool $condition */
    private function until(Closure $condition): void
    {
        for ($index = 0; $index < 500; $index++) {
            if ($condition()) {
                return;
            }
            self::checkpoint();
        }
        self::fail('the scripted event was not reached within 500 event-loop checkpoints');
    }

    private static function checkpoint(): void
    {
        $hop = new DeferredFuture();
        EventLoop::queue(static fn() => $hop->complete());
        $hop->getFuture()->await();
    }
}
