<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\IncomingChunkResult;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\SlowConsumerException;
use IDCT\NATS\Tests\Support\HeldUpDelivery;
use IDCT\NATS\Tests\Support\OwnsTestResources;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\WatchedTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\async;
use function Amp\delay;

/**
 * Your own read already waiting - on the socket, or for the read slot - is woken when another fiber's read stops a
 * subscription at a handler failure, and returns zero frames; your next read continues that subscription before it
 * reads (#199). Over ReconnectingTransport seen through WatchedTransport, heartbeat off: a read (your own, or a
 * SubscriptionQueue poll under handlerErrorsFailOperations) takes a chunk and is held up in a handler that awaits
 * (HeldUpDelivery) ahead of the failing one, while your read takes the socket or waits for the slot; no server bytes
 * follow. Up to 2.26.x that read waited for the server's next bytes or its own cancellation. Every client, operation and
 * held delivery is owned by the test.
 */
final class StoppedSubscriptionReadWakeupTest extends TestCase
{
    use OwnsTestResources;

    protected function tearDown(): void
    {
        $this->releaseOwnedResources();
    }

    /** @return iterable<string, array{bool, bool, bool}> */
    public static function socketCases(): iterable
    {
        foreach ([false, true] as $operation) {
            foreach ([false, true] as $overflow) {
                foreach ([false, true] as $chunkResult) {
                    yield ($operation ? 'poll' : 'own read') . ', ' . ($overflow ? 'escaped overflow' : 'handler failure') . ', ' . ($chunkResult ? 'readIncoming' : 'processIncoming') => [$operation, $overflow, $chunkResult];
                }
            }
        }
    }

    /**
     * Your processIncoming() or readIncoming() waiting on the socket wakes when the held-up read - your own, or a poll
     * under handlerErrorsFailOperations - stops "s" at s1 (a handler failure or an escaped overflow): it returns zero
     * frames, no bytes consumed, its socket read cancelled, before any server bytes. The next read throws s2's failure
     * without reading, and the one after continues s3 and reads the server's PING.
     */
    #[DataProvider('socketCases')]
    public function testAStopWakesASocketReadBeforeAnyServerBytes(bool $operation, bool $overflow, bool $chunkResult): void
    {
        [$connection, $transport, $watched, $hold, $seen, $failure, $trigger, $errors] = $this->heldFailure($operation, $overflow);
        $read = $this->resources()->ownOperation($chunkResult ? $connection->readIncoming() : $connection->processIncoming());
        $this->awaitSocket($watched);
        $reads = $watched->reads;
        $hold->end('test');
        $this->assertFailure($trigger, $failure);
        // One loop turn lets queued cancellation callbacks finish; this is not a wall-clock assertion.
        delay(0);
        self::assertTrue($read->isComplete(), 'the application read must finish without server bytes');
        $result = $read->await();
        if ($chunkResult) {
            self::assertInstanceOf(IncomingChunkResult::class, $result);
            self::assertSame(0, $result->frames);
            self::assertFalse($result->consumedBytes);
        } else {
            self::assertSame(0, $result);
        }
        self::assertSame(['r1', 's1'], $seen->getArrayCopy(), 'wake and return leaves continuation to the next call');
        self::assertSame(1, $watched->cancelledReads);
        self::assertSame(0, $watched->readsUnderWay);

        // The next failure is thrown by the next read, without touching the socket.
        $second = new \RuntimeException('s2 failure');
        $seen->secondFailure = $second;
        $this->assertFailure($connection->processIncoming(), $second);
        self::assertSame($reads, $watched->reads);
        $transport->pushFrame("PING\r\n");
        self::assertSame(1, $connection->processIncoming()->await());
        self::assertSame(['r1', 's1', 's2', 's3'], $seen->getArrayCopy());
        self::assertSame([], $errors->getArrayCopy());
    }

    /** @return iterable<string, array{bool}> */
    public static function failureKinds(): iterable
    {
        yield 'handler failure' => [false];
        yield 'escaped overflow' => [true];
    }

    /**
     * A third read of yours waiting for the slot an unrelated operation's read holds wakes at the stop, while that
     * operation, which has no such wake-up, keeps the socket. Your next read continues s2 and s3 and then waits as
     * usual on a fresh wake-up.
     */
    #[DataProvider('failureKinds')]
    public function testAStopWakesTheSlotWaitWithoutCancellingItsOperationOwner(bool $overflow): void
    {
        [$connection, $transport, $watched, $hold, $seen, $failure, $trigger] = $this->heldFailure(false, $overflow);
        $jobs = $connection->subscribe('jobs', static function (): void {})->await();
        $ownerStop = new DeferredCancellation();
        $owner = $this->resources()->ownOperation(
            $connection->readIncomingForOperation($ownerStop->getCancellation(), ownSid: $jobs),
            $ownerStop->cancel(...),
        );
        $this->awaitSocket($watched);
        $reads = $watched->reads;
        $waiting = new StoppedReadWakeupObservedCancellation();
        $read = $this->resources()->ownOperation($connection->readIncoming($waiting), $waiting->cancel(...));
        $waiting->subscribed->getFuture()->await(new TimeoutCancellation(2));
        self::assertSame($reads, $watched->reads, 'the third read waits for the occupied slot');

        $hold->end('test');
        $this->assertFailure($trigger, $failure);
        delay(0);
        self::assertTrue($read->isComplete(), 'the slot waiter must wake while its owner still waits');
        self::assertFalse($owner->isComplete(), 'an unrelated operation has no stopped-subscription wake-up');
        self::assertSame(1, $watched->readsUnderWay);
        self::assertSame(0, $read->await()->frames);
        self::assertSame(0, $watched->cancelledReads);
        self::assertSame(['r1', 's1'], $seen->getArrayCopy());

        // Next application read continues s2/s3, then joins the same owner.
        $nextStop = new StoppedReadWakeupObservedCancellation();
        $next = $this->resources()->ownOperation($connection->readIncoming($nextStop), $nextStop->cancel(...));
        $nextStop->subscribed->getFuture()->await(new TimeoutCancellation(2));
        delay(0);
        self::assertSame(['r1', 's1', 's2', 's3'], $seen->getArrayCopy());
        self::assertFalse($next->isComplete(), 'a fresh wake-up must wait normally');
        $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $jobs, 'j1'));
        self::assertSame(1, $owner->await(new TimeoutCancellation(2))->frames);
        self::assertSame(0, $next->await(new TimeoutCancellation(2))->frames);
    }

    /** One stop wakes both of your reads: the one on the socket and the one waiting for its slot. */
    #[DataProvider('failureKinds')]
    public function testAStopAlsoWakesAllApplicationWaitersSharingTheSocketAndItsSlot(bool $overflow): void
    {
        [$connection, $transport, $watched, $hold, $seen, $failure, $trigger] = $this->heldFailure(false, $overflow);
        $socket = $this->resources()->ownOperation($connection->readIncoming());
        $this->awaitSocket($watched);
        $reads = $watched->reads;
        $waiting = new StoppedReadWakeupObservedCancellation();
        $slot = $this->resources()->ownOperation($connection->readIncoming($waiting), $waiting->cancel(...));
        $waiting->subscribed->getFuture()->await(new TimeoutCancellation(2));
        $hold->end('test');
        $this->assertFailure($trigger, $failure);
        delay(0);
        self::assertTrue($socket->isComplete(), 'the socket owner must wake');
        self::assertTrue($slot->isComplete(), 'the slot waiter must wake too');
        self::assertSame(0, $socket->await()->frames);
        self::assertSame(0, $slot->await()->frames);
        self::assertSame($reads, $watched->reads);
        self::assertSame(1, $watched->cancelledReads);
        $transport->pushFrame("PING\r\n");
        self::assertSame(1, $connection->processIncoming()->await());
        self::assertSame(['r1', 's1', 's2', 's3'], $seen->getArrayCopy());
    }

    /** @return iterable<string, array{bool}> */
    public static function readKinds(): iterable
    {
        yield 'processIncoming' => [false];
        yield 'readIncoming' => [true];
    }

    /**
     * Guard against a spin: after a stop made while no read waited, three of your reads in a row each wait on the
     * socket (one transport read each) until the server's PING; a fired wake-up is never reused.
     */
    #[DataProvider('readKinds')]
    public function testAStopBeforeTheNextReadNeverReusesAFiredWakeup(bool $chunkResult): void
    {
        [$connection, $transport, $watched] = $this->connection();
        $seen = [];
        $failure = new \RuntimeException('first failure');
        $sid = $connection->subscribe('s', static function (NatsMessage $message) use (&$seen, $failure): void {
            $seen[] = $message->payload;
            if ($message->payload === 's1') {
                throw $failure;
            }
        })->await();
        $transport->pushFrame(ReconnectingTransport::msgFrame('s', $sid, 's1') . ReconnectingTransport::msgFrame('s', $sid, 's2'));
        $this->assertFailure($connection->processIncoming(), $failure);
        $reads = $watched->reads;
        for ($attempt = 0; $attempt < 3; $attempt++) {
            $stop = new DeferredCancellation();
            $read = $this->resources()->ownOperation(
                $chunkResult ? $connection->readIncoming($stop->getCancellation()) : $connection->processIncoming($stop->getCancellation()),
                $stop->cancel(...),
            );
            $this->awaitSocket($watched);
            delay(0);
            self::assertFalse($read->isComplete(), 'an old stop must not produce immediate empty reads');
            self::assertSame(['s1', 's2'], $seen);
            self::assertSame($reads + $attempt + 1, $watched->reads);
            self::assertSame(1, $watched->readsUnderWay);
            $transport->pushFrame("PING\r\n");
            $result = $read->await(new TimeoutCancellation(2));
            self::assertSame(1, $result instanceof IncomingChunkResult ? $result->frames : $result);
        }
    }

    /** @return iterable<string, array{bool, bool}> */
    public static function waitKinds(): iterable
    {
        foreach ([false, true] as $slotWait) {
            foreach ([false, true] as $stopFirst) {
                yield ($slotWait ? 'slot wait' : 'socket wait') . ', ' . ($stopFirst ? 'stop first' : 'caller first') => [$slotWait, $stopFirst];
            }
        }
    }

    /**
     * When your cancellation and the stop both fire, in either order, on the socket or the slot wait, the read throws
     * CancelledException with your cancellation's reason, not a zero-frame result.
     */
    #[DataProvider('waitKinds')]
    public function testCallerCancellationWinsWhenTheStopWakeupAlsoFires(bool $slotWait, bool $stopFirst): void
    {
        [$connection, , $watched, $hold, $seen, $failure, $trigger] = $this->heldFailure(false, false);
        $ownerStop = new DeferredCancellation();
        if ($slotWait) {
            $owner = $this->resources()->ownOperation($connection->readIncoming($ownerStop->getCancellation()), $ownerStop->cancel(...));
            $this->awaitSocket($watched);
        }
        $waiting = new StoppedReadWakeupObservedCancellation();
        $read = $this->resources()->ownOperation($connection->readIncoming($waiting), $waiting->cancel(...));
        $waiting->subscribed->getFuture()->await(new TimeoutCancellation(2));
        $reason = new \RuntimeException('caller cancellation reason');
        // Queuing caller cancellation lets the stop fire first. Its composite callback then wins, but the
        // caller is also cancelled by the time the read resumes: the read must restore the caller's reason.
        $seen->beforeFailure = $stopFirst
            ? static fn() => EventLoop::queue(static fn() => $waiting->cancel($reason))
            : static fn() => $waiting->cancel($reason);
        $hold->end('test');
        $this->assertFailure($trigger, $failure);
        try {
            $read->await(new TimeoutCancellation(2));
            self::fail('caller cancellation must escape');
        } catch (CancelledException $cancelled) {
            self::assertSame($reason, $cancelled->getPrevious(), 'the caller cancellation reason must win');
            self::assertTrue($read->isComplete(), 'the read itself must settle, not only the test wait');
        }
        if (isset($owner)) {
            $ownerStop->cancel();
            try {
                $owner->await(new TimeoutCancellation(2));
            } catch (CancelledException) {
                self::assertTrue($owner->isComplete());
            }
        }
        self::assertSame(['r1', 's1'], $seen->getArrayCopy());
    }

    /**
     * Control: under the default options an operation's read reports the handler failure and delivers the rest at
     * once, so no subscription is stopped and your idle read is not woken.
     */
    public function testAReportedFailureDeliversItsRemainderAndDoesNotWakeAnApplicationRead(): void
    {
        [$connection, $transport, $watched, $errors] = $this->connection(false);
        $hold = new HeldUpDelivery(3.0);
        $this->resources()->onRelease(static fn() => $hold->end('cleanup'), 'held reporting handler');
        $r = $connection->subscribe('r', $hold->handler())->await();
        $seen = [];
        $s = $connection->subscribe('s', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = $message->payload;
            if ($message->payload === 's1') {
                throw new \RuntimeException('reported failure');
            }
        })->await();
        $jobs = $connection->subscribeQueue('jobs')->await();
        $transport->pushFrame(ReconnectingTransport::msgFrame('r', $r, 'r1') . ReconnectingTransport::msgFrame('s', $s, 's1') . ReconnectingTransport::msgFrame('s', $s, 's2') . ReconnectingTransport::msgFrame('jobs', $jobs->sid, 'j1'));
        $poll = $this->resources()->ownOperation(async(static fn() => $jobs->setTimeout(3)->next()));
        $hold->began->getFuture()->await(new TimeoutCancellation(2));
        $read = $this->resources()->ownOperation($connection->processIncoming());
        $this->awaitSocket($watched);
        $hold->end('test');
        self::assertSame('j1', $poll->await(new TimeoutCancellation(2))?->payload);
        delay(0);
        self::assertSame(['s1', 's2'], $seen);
        self::assertFalse($read->isComplete());
        self::assertSame(0, $watched->cancelledReads);
        self::assertCount(1, $errors);
        $reported = $errors[0];
        self::assertNotNull($reported);
        self::assertSame('reported failure', $reported->getMessage());
        $transport->pushFrame("PING\r\n");
        self::assertSame(1, $read->await(new TimeoutCancellation(2)));
    }

    /**
     * @return array{NatsClient, ReconnectingTransport, WatchedTransport, \ArrayObject<int, \Throwable>}
     */
    private function connection(bool $failOperations = true): array
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $errors = new StoppedReadWakeupErrors();
        $connection = $this->own(new NatsClient(new NatsOptions(
            pingIntervalSeconds: 0,
            handlerErrorsFailOperations: $failOperations,
            errorListener: static function (\Throwable $error) use ($errors): void {
                $errors[] = $error;
            },
        ), $watched));
        $connection->connect()->await();
        $this->resources()->expectIdle(static fn(): bool => $watched->readsUnderWay === 0, 'scripted transport reads');

        return [$connection, $transport, $watched, $errors];
    }

    /**
     * @return array{NatsClient, ReconnectingTransport, WatchedTransport, HeldUpDelivery, StoppedReadWakeupSeen, \Throwable, Future<mixed>, \ArrayObject<int, \Throwable>}
     */
    private function heldFailure(bool $operation, bool $overflow): array
    {
        [$connection, $transport, $watched, $errors] = $this->connection();
        $seen = new StoppedReadWakeupSeen();
        $failure = $overflow ? new SlowConsumerException(999) : new \RuntimeException('s1 failure');
        $hold = new HeldUpDelivery(3.0);
        $this->resources()->onRelease(static fn() => $hold->end('cleanup'), 'held failing delivery');
        $r = $connection->subscribe('r', static function (NatsMessage $message) use ($seen, $hold): void {
            $seen[] = $message->payload;
            $hold->holdUp();
        })->await();
        $s = $connection->subscribe('s', static function (NatsMessage $message) use ($seen, $failure): void {
            $seen[] = $message->payload;
            if ($message->payload === 's1') {
                if ($seen->beforeFailure !== null) {
                    ($seen->beforeFailure)();
                }
                throw $failure;
            }
            if ($message->payload === 's2' && $seen->secondFailure !== null) {
                throw $seen->secondFailure;
            }
        })->await();
        $jobs = $connection->subscribeQueue('jobs')->await();
        $transport->pushFrame(ReconnectingTransport::msgFrame('r', $r, 'r1') . ReconnectingTransport::msgFrame('s', $s, 's1') . ReconnectingTransport::msgFrame('s', $s, 's2') . ReconnectingTransport::msgFrame('s', $s, 's3'));
        $trigger = $this->resources()->ownOperation($operation ? async(static fn() => $jobs->setTimeout(3)->next()) : $connection->processIncoming());
        $hold->began->getFuture()->await(new TimeoutCancellation(2));

        return [$connection, $transport, $watched, $hold, $seen, $failure, $trigger, $errors];
    }

    private function awaitSocket(WatchedTransport $watched): void
    {
        /** @var DeferredFuture<null> $taken */
        $taken = new DeferredFuture();
        $watched->onRead = static function () use ($taken): void {
            $taken->complete();
        };
        try {
            $taken->getFuture()->await(new TimeoutCancellation(2));
        } finally {
            $watched->onRead = null;
        }
    }

    /** @param Future<mixed> $future */
    private function assertFailure(Future $future, \Throwable $expected): void
    {
        $actual = null;
        try {
            $future->await(new TimeoutCancellation(2));
        } catch (\Throwable $failure) {
            $actual = $failure;
        }
        self::assertSame($expected, $actual);
    }
}

/** @extends \ArrayObject<int, string> */
final class StoppedReadWakeupSeen extends \ArrayObject
{
    public ?\Throwable $secondFailure = null;
    /** @var (\Closure(): void)|null */
    public ?\Closure $beforeFailure = null;
}

/** @extends \ArrayObject<int, \Throwable> */
final class StoppedReadWakeupErrors extends \ArrayObject {}

/** Signals when the read has subscribed to its caller cancellation, without using a timer to stage it. */
final class StoppedReadWakeupObservedCancellation implements Cancellation
{
    /** @var DeferredFuture<null> */
    public readonly DeferredFuture $subscribed;
    private readonly DeferredCancellation $inner;

    public function __construct()
    {
        $this->subscribed = new DeferredFuture();
        $this->inner = new DeferredCancellation();
    }

    public function cancel(?\Throwable $reason = null): void
    {
        $this->inner->cancel($reason);
    }

    public function subscribe(\Closure $callback): string
    {
        $id = $this->inner->getCancellation()->subscribe($callback);
        if (!$this->subscribed->isComplete()) {
            $this->subscribed->complete();
        }

        return $id;
    }

    public function unsubscribe(string $id): void
    {
        $this->inner->getCancellation()->unsubscribe($id);
    }

    public function isRequested(): bool
    {
        return $this->inner->isCancelled();
    }

    public function throwIfRequested(): void
    {
        $this->inner->getCancellation()->throwIfRequested();
    }
}
