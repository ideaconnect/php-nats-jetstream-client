<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\JetStream\Consumers\PullConsumerIterator;
use IDCT\NATS\Tests\Support\OwnsTestResources;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\WatchedTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Amp\delay;

/** Regression coverage for receipt-time pin retention (#208). */
final class PullConsumerPinRetentionTest extends TestCase
{
    use OwnsTestResources;

    protected function tearDown(): void
    {
        $this->releaseOwnedResources();
    }

    /** @return iterable<string, array{string}> */
    public static function endings(): iterable
    {
        foreach (['fatal-finite', 'eof-no-wait', 'handler-failure', 'inbox-rejection', 'stop', 'disconnect', 'client-drain'] as $ending) {
            yield $ending => [$ending];
        }
    }

    /**
     * A grouped run whose first pull holds m-1 and m-2 under pin p1, its batch still incomplete, ends: a fatal -ERR in a
     * finite run, an EOF with waiting for the reconnect disabled, another subscription's handler failing it, a rejection
     * of its inbox, stop(), disconnect() or the client's drain(). Each ending keeps its delivery count and failure, and
     * the next handle() on the same iterator sends p1 in its first pull. Up to 2.25.8 the pin was taken only when the
     * batch retired, so the restart pulled without it and the server held the pull until the pin timed out.
     */
    #[DataProvider('endings')]
    public function testRestartKeepsThePinOfAnUnretiredFirstPull(string $ending): void
    {
        [$transport, $watched, $client, $server] = $this->client($ending !== 'eof-no-wait');
        $otherSid = $client->subscribe('other', static function (): void {
            throw new \RuntimeException('other handler failed');
        })->await();
        $iterator = $this->iterator($client);
        if ($ending === 'fatal-finite') {
            $iterator->setIterations(1);
        }
        $handled = new Issue208HandledLog();
        $run = $this->startRun($iterator, static function (NatsMessage $message) use ($handled): void {
            $handled->payloads[] = $message->payload;
        });
        $this->waitUntil(static fn(): bool => count($server->requests) === 1 && $watched->readsUnderWay === 1);
        $this->sendAndWait($transport, $watched, self::message($server->requests[0]['sid'], 'm-1', 'p1'));
        $this->sendAndWait($transport, $watched, self::message($server->requests[0]['sid'], 'm-2', 'p1'));
        self::assertSame([], $handled->payloads, 'the first batch is still incomplete');

        switch ($ending) {
            case 'fatal-finite':
                $transport->pushFrame("-ERR 'Stale Connection'\r\n");
                break;
            case 'eof-no-wait':
                $transport->dropConnection();
                break;
            case 'handler-failure':
                $transport->pushFrame(ReconnectingTransport::msgFrame('other', $otherSid, 'x'));
                break;
            case 'inbox-rejection':
                $separator = strrpos($server->requests[0]['reply'], '.');
                if ($separator === false) {
                    throw new \LogicException('A pull reply must have a token suffix');
                }
                $inbox = substr($server->requests[0]['reply'], 0, $separator) . '.*';
                $transport->pushFrame(sprintf("-ERR 'Permissions Violation for Subscription to \"%s\" (sid \"%d\")'\r\n", $inbox, $server->requests[0]['sid']));
                break;
            case 'stop':
                $iterator->stop();
                break;
            case 'disconnect':
                $client->disconnect()->await(new TimeoutCancellation(3));
                break;
            case 'client-drain':
                $client->drain()->await(new TimeoutCancellation(3));
                break;
        }
        [$processed, $failure] = self::settle($run);
        if (in_array($ending, ['stop', 'client-drain'], true)) {
            self::assertNull($failure);
            self::assertSame($ending === 'stop' ? 0 : 2, $processed);
        } else {
            self::assertNotNull($failure);
            self::assertStringContainsString(match ($ending) {
                'fatal-finite' => "Server sent error frame: 'Stale Connection'",
                'handler-failure' => 'other handler failed',
                'inbox-rejection' => 'Permissions Violation',
                default => 'Connection is not open',
            }, $failure->getMessage());
        }
        self::assertSame(in_array($ending, ['stop', 'disconnect'], true) ? [] : ['m-1', 'm-2'], $handled->payloads);
        if ($client->state() === ConnectionState::Closed) {
            $client->connect()->await(new TimeoutCancellation(3));
        }
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open);
        self::assertSame('p1', $this->restartPin($iterator, $server));
    }

    /** Guard: a pull that retires full under a pin already passed the pin on to the next run. */
    public function testAFullRetiredPullAlreadyKeepsItsPin(): void
    {
        [$transport, $watched, $client, $server] = $this->client();
        $iterator = $this->iterator($client)->setBatching(1)->setIterations(1);
        $run = $this->startRun($iterator);
        $this->waitUntil(static fn(): bool => count($server->requests) === 1 && $watched->readsUnderWay === 1);
        $transport->pushFrame(self::message($server->requests[0]['sid'], 'm-1', 'p1'));
        self::assertSame([1, null], self::settle($run));
        self::assertSame('p1', $this->restartPin($iterator, $server));
    }

    /**
     * stop() and handle() again while the old run's pull write has its completion held (its bytes reached the server,
     * which answered with m-1 under p1): the new run's first pull carries p1 before the old run has even reached its
     * cleanup. The pin is remembered on receipt, not on any exit path of the old run.
     */
    public function testStopThenRestartDoesNotWaitForTheOldPullWriteToComplete(): void
    {
        [$transport, $watched, $client, $server] = $this->client();
        $transport->holdCompletionOfNextWriteContaining('PUB $JS.API.CONSUMER.MSG.NEXT.S.C ', 30);
        $this->resources()->onRelease($transport->releaseHeldCompletions(...), 'the old pull completion');
        $iterator = $this->iterator($client);
        $oldRun = $this->startRun($iterator);
        $this->waitUntil(static fn(): bool => $transport->completionsHeld() === 1 && count($server->requests) === 1);
        $transport->pushFrame(self::message($server->requests[0]['sid'], 'm-1', 'p1'));
        // Its PONG follows the data on this ordered transport; the held write's bytes already arrived.
        $client->flush()->await(new TimeoutCancellation(3));
        $iterator->stop();
        self::assertFalse($oldRun->isComplete());
        $restart = $this->startRun($iterator);
        $this->waitUntil(static fn(): bool => count($server->requests) >= 2);
        $pin = $server->requests[1]['payload']['id'] ?? null;
        $iterator->stop();
        $transport->releaseHeldCompletions();
        self::assertSame([0, null], self::settle($oldRun));
        self::assertSame([0, null], self::settle($restart));
        self::assertSame('p1', $pin, 'a new run can issue before the old run reaches finally');
    }

    /**
     * A 423 clears p1 and the run pulls again without a pin; the server answers under a new pin p2 and the run then
     * fails on a fatal -ERR before that batch retires. The restart sends p2.
     */
    public function testFailureDuringTheFirstPullAfterA423KeepsTheNewPin(): void
    {
        [$transport, $watched, $client, $server] = $this->client();
        $iterator = $this->iterator($client)->setBatching(1)->setIterations(1);
        $seed = $this->startRun($iterator);
        $this->waitUntil(static fn(): bool => count($server->requests) === 1 && $watched->readsUnderWay === 1);
        $transport->pushFrame(self::message($server->requests[0]['sid'], 'seed', 'p1'));
        self::assertSame([1, null], self::settle($seed));

        $iterator->setBatching(3)->setIterations(2);
        $run = $this->startRun($iterator);
        $this->waitUntil(static fn(): bool => count($server->requests) === 2 && $watched->readsUnderWay === 1);
        $transport->pushFrame(self::pinStatusFrame($server->requests[1]['reply'], $server->requests[1]['sid'], 423));
        $this->waitUntil(static fn(): bool => count($server->requests) === 3 && $watched->readsUnderWay === 1);
        self::assertArrayNotHasKey('id', $server->requests[2]['payload']);
        $this->sendAndWait($transport, $watched, self::message($server->requests[2]['sid'], 'new', 'p2'));
        $transport->pushFrame("-ERR 'Stale Connection'\r\n");
        [, $failure] = self::settle($run);
        self::assertInstanceOf(ConnectionException::class, $failure);
        self::assertSame('p2', $this->restartPin($iterator, $server));
    }

    /**
     * Two runs of one iterator: the newer one's 423 clears p1 and it is then served under p2. The older run's 423 for
     * its p1 request arrives later and must not clear p2: a 423 clears only the pin its own request sent.
     */
    public function testAnOlderRunsDelayed423DoesNotClearTheNewRunsPin(): void
    {
        [$transport, $watched, $client, $server] = $this->client();
        $iterator = $this->iterator($client)->setBatching(1)->setIterations(1);
        $seed = $this->startRun($iterator);
        $this->waitUntil(static fn(): bool => count($server->requests) === 1 && $watched->readsUnderWay === 1);
        $transport->pushFrame(self::message($server->requests[0]['sid'], 'seed', 'p1'));
        self::assertSame([1, null], self::settle($seed));
        $older = $this->startRun($iterator);
        $this->waitUntil(static fn(): bool => count($server->requests) === 2 && $watched->readsUnderWay === 1);
        $iterator->setIterations(2);
        $newer = $this->startRun($iterator);
        $this->waitUntil(static fn(): bool => count($server->requests) === 3);
        $transport->pushFrame(self::pinStatusFrame($server->requests[2]['reply'], $server->requests[2]['sid'], 423));
        $this->waitUntil(static fn(): bool => count($server->requests) === 4);
        $transport->pushFrame(self::message($server->requests[3]['sid'], 'new', 'p2'));
        self::assertSame([1, null], self::settle($newer));
        $transport->pushFrame(self::pinStatusFrame($server->requests[1]['reply'], $server->requests[1]['sid'], 423));
        self::assertSame([0, null], self::settle($older));
        self::assertSame('p2', $this->restartPin($iterator, $server));
    }

    /**
     * A run that starts with the iterator's pin p1 pipelines at once. Another run's 423 then clears p1, and the first
     * run's own 423 for p1 is ignored, since p1 is no longer the live pin. Pinless, the run knows from the inherited pin
     * that its group pins, so it refills one pull at a time instead of fanning out pin-less pulls that would race the
     * server's assignment of a new pin. Added to the issue's cases: the inherited $everPinned had no case of its own.
     */
    public function testARunThatInheritedThePinStaysSerialWhenAnotherRunLosesIt(): void
    {
        [$transport, $watched, $client, $server] = $this->client();
        $iterator = $this->iterator($client)->setBatching(1)->setIterations(1);
        $seed = $this->startRun($iterator);
        $this->waitUntil(static fn(): bool => count($server->requests) === 1 && $watched->readsUnderWay === 1);
        $transport->pushFrame(self::message($server->requests[0]['sid'], 'seed', 'p1'));
        self::assertSame([1, null], self::settle($seed));

        $iterator->setBatching(3)->setIterations(null);
        $inheriting = $this->startRun($iterator);
        $this->waitUntil(static fn(): bool => count($server->requests) === 3 && $watched->readsUnderWay === 1);
        self::assertSame(['p1', 'p1'], [$server->requests[1]['payload']['id'] ?? null, $server->requests[2]['payload']['id'] ?? null], 'the inherited pin lets the run pipeline at once');

        $iterator->setIterations(1);
        $other = $this->startRun($iterator);
        $this->waitUntil(static fn(): bool => count($server->requests) === 4);
        $transport->pushFrame(self::pinStatusFrame($server->requests[3]['reply'], $server->requests[3]['sid'], 423));
        self::assertSame([0, null], self::settle($other));

        // The inheriting run's own 423 names p1, no longer the live pin: ignored. Pinless, the run refills serially.
        $this->sendAndWait($transport, $watched, self::pinStatusFrame($server->requests[1]['reply'], $server->requests[1]['sid'], 423));
        self::assertCount(4, $server->requests, 'with one pull still in flight the pinless run issues no pin-less fan-out');
        $iterator->stop();
        self::assertSame([0, null], self::settle($inheriting));
        self::assertNull($this->restartPin($iterator, $server));
    }

    /** @return iterable<string, array{bool}> */
    public static function pinOrigins(): iterable
    {
        yield 'captured in this run' => [false];
        yield 'retained from a previous run' => [true];
    }

    /**
     * After 423s clear p1, a pin captured in this run or inherited from a previous run, the run re-acquires serially:
     * receiving p2 on the first message of the new batch does not let it fan out. Only once that batch retires do two
     * further pulls go out, both with p2.
     */
    #[DataProvider('pinOrigins')]
    public function testPinRecaptureStaysSerialUntilItsFirstBatchRetires(bool $retainedFromPreviousRun): void
    {
        [$transport, $watched, $client, $server] = $this->client();
        $iterator = $this->iterator($client);
        if ($retainedFromPreviousRun) {
            $iterator->setIterations(1);
        }
        $handled = new Issue208HandledLog();
        $handler = static function (NatsMessage $message) use ($handled): void {
            $handled->payloads[] = $message->payload;
        };
        $run = $this->startRun($iterator, $handler);
        $this->waitUntil(static fn(): bool => count($server->requests) === 1 && $watched->readsUnderWay === 1);
        $transport->pushFrame(
            self::message($server->requests[0]['sid'], 'm-1', 'p1')
            . self::message($server->requests[0]['sid'], 'm-2', 'p1')
            . self::message($server->requests[0]['sid'], 'm-3', 'p1'),
        );
        if ($retainedFromPreviousRun) {
            self::assertSame([3, null], self::settle($run));
            $iterator->setIterations(null);
            $run = $this->startRun($iterator, $handler);
        }
        $this->waitUntil(static fn(): bool => count($server->requests) === 3 && $watched->readsUnderWay === 1);
        $transport->pushFrame(
            self::pinStatusFrame($server->requests[1]['reply'], $server->requests[1]['sid'], 423)
            . self::pinStatusFrame($server->requests[2]['reply'], $server->requests[2]['sid'], 423),
        );
        $this->waitUntil(static fn(): bool => count($server->requests) === 4 && $watched->readsUnderWay === 1);
        self::assertArrayNotHasKey('id', $server->requests[3]['payload']);
        $this->sendAndWait($transport, $watched, self::message($server->requests[3]['sid'], 'm-4', 'p2'));
        self::assertSame(['m-1', 'm-2', 'm-3'], $handled->payloads);
        self::assertCount(4, $server->requests, 'remembering p2 must not fan out before its first batch retires');
        $transport->pushFrame(
            self::message($server->requests[3]['sid'], 'm-5', 'p2')
            . self::message($server->requests[3]['sid'], 'm-6', 'p2'),
        );
        $this->waitUntil(static fn(): bool => count($server->requests) === 6 && $watched->readsUnderWay === 1);
        self::assertSame('p2', $server->requests[4]['payload']['id']);
        self::assertSame('p2', $server->requests[5]['payload']['id']);
        $iterator->stop();
        self::assertSame([$retainedFromPreviousRun ? 3 : 6, null], self::settle($run));
    }

    /**
     * An older run holds a partial batch received under p1 when a newer run's 423 rejects p1. The older run's pull then
     * expires and its buffer is delivered: delivery must not take p1 from the buffered message again.
     */
    public function testDeliveringAnOldBufferDoesNotRestoreAPinRejectedByAnotherRun(): void
    {
        [$transport, $watched, $client, $server] = $this->client();
        $iterator = $this->iterator($client)->setBatching(1)->setIterations(1);
        $seed = $this->startRun($iterator);
        $this->waitUntil(static fn(): bool => count($server->requests) === 1 && $watched->readsUnderWay === 1);
        $transport->pushFrame(self::message($server->requests[0]['sid'], 'seed', 'p1'));
        self::assertSame([1, null], self::settle($seed));

        $iterator->setBatching(3);
        $older = $this->startRun($iterator);
        $this->waitUntil(static fn(): bool => count($server->requests) === 2 && $watched->readsUnderWay === 1);
        $this->sendAndWait($transport, $watched, self::message($server->requests[1]['sid'], 'old', 'p1'));
        $newer = $this->startRun($iterator);
        $this->waitUntil(static fn(): bool => count($server->requests) === 3);
        $transport->pushFrame(self::pinStatusFrame($server->requests[2]['reply'], $server->requests[2]['sid'], 423));
        self::assertSame([0, null], self::settle($newer));
        // The old pull expires with its partial buffer after another run invalidated p1.
        $transport->pushFrame(self::pinStatusFrame($server->requests[1]['reply'], $server->requests[1]['sid'], 408));
        self::assertSame([1, null], self::settle($older));
        self::assertNull($this->restartPin($iterator, $server), 'retirement must not re-read p1 from old buffered data');
    }

    /** @return iterable<string, array{string}> */
    public static function framesWithoutANewDataPin(): iterable
    {
        foreach (['missing-header', 'empty-header', 'heartbeat-with-pin'] as $shape) {
            yield $shape => [$shape];
        }
    }

    /**
     * A data message without a pin header or with an empty one, and a heartbeat status frame carrying a pin header,
     * leave the retained p1 unchanged.
     */
    #[DataProvider('framesWithoutANewDataPin')]
    public function testOnlyANonEmptyDataPinChangesTheRetainedPin(string $shape): void
    {
        [$transport, $watched, $client, $server] = $this->client();
        $iterator = $this->iterator($client)->setBatching(1)->setIterations(1);
        $seed = $this->startRun($iterator);
        $this->waitUntil(static fn(): bool => count($server->requests) === 1 && $watched->readsUnderWay === 1);
        $transport->pushFrame(self::message($server->requests[0]['sid'], 'seed', 'p1'));
        self::assertSame([1, null], self::settle($seed));

        $iterator->setBatching(3);
        $run = $this->startRun($iterator);
        $this->waitUntil(static fn(): bool => count($server->requests) === 2 && $watched->readsUnderWay === 1);
        $request = $server->requests[1];
        $frame = match ($shape) {
            'missing-header' => self::message($request['sid'], 'm-1', null),
            'empty-header' => self::message($request['sid'], 'm-1', ''),
            default => ReconnectingTransport::hmsgFrame($request['reply'], $request['sid'], "NATS/1.0 100 Idle Heartbeat\r\nNats-Pin-Id: unexpected\r\n\r\n", ''),
        };
        $this->sendAndWait($transport, $watched, $frame);
        $iterator->stop();
        self::assertSame([0, null], self::settle($run));
        self::assertSame('p1', $this->restartPin($iterator, $server));
    }

    /** Guard: a group whose messages carry no pin (overflow/prioritized) fans out after its first delivery, pinless. */
    public function testAGroupWhoseMessagesHaveNoPinStillFansOut(): void
    {
        [$transport, $watched, $client, $server] = $this->client();
        $iterator = $this->iterator($client)->setBatching(1);
        $run = $this->startRun($iterator);
        $this->waitUntil(static fn(): bool => count($server->requests) === 1 && $watched->readsUnderWay === 1);
        $transport->pushFrame(self::message($server->requests[0]['sid'], 'm-1', null));
        $this->waitUntil(static fn(): bool => count($server->requests) === 3 && $watched->readsUnderWay === 1);
        $iterator->stop();
        self::assertSame([1, null], self::settle($run));
        self::assertSame([null, null, null], array_map(static fn(array $request): ?string => $request['payload']['id'] ?? null, $server->requests));
        self::assertNull($this->restartPin($iterator, $server));
    }

    /** An ungrouped run takes no pin, even from a data message that carries one. */
    public function testAnUngroupedRunDoesNotCaptureAnUnexpectedPinHeader(): void
    {
        [$transport, $watched, $client, $server] = $this->client();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000);
        $run = $this->startRun($iterator);
        $this->waitUntil(static fn(): bool => count($server->requests) === 1 && $watched->readsUnderWay === 1);
        $this->sendAndWait($transport, $watched, self::message($server->requests[0]['sid'], 'm-1', 'unexpected'));
        $iterator->stop();
        self::assertSame([0, null], self::settle($run));
        self::assertNull($this->restartPin($iterator, $server));
    }

    /** @return array{ReconnectingTransport, WatchedTransport, NatsClient, Issue208PullLog} */
    private function client(bool $waitForReconnect = true): array
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->own(new NatsClient(new NatsOptions(
            reconnectEnabled: true,
            waitForReconnect: $waitForReconnect,
            maxReconnectAttempts: 3,
            reconnectDelayMs: 1,
            reconnectMaxDelayMs: 1,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
            handlerErrorsFailOperations: true,
        ), $watched));
        $client->connect()->await(new TimeoutCancellation(3));
        $server = new Issue208PullLog();
        $transport->responder = static function (string $subject, ?string $replyTo, string $payload) use ($server, $transport): array {
            if ($subject === '$JS.API.CONSUMER.MSG.NEXT.S.C' && $replyTo !== null) {
                $sid = $transport->sidFor($replyTo);
                if ($sid === null) {
                    throw new \LogicException('The pull must have a subscribed inbox');
                }
                $request = json_decode($payload, flags: JSON_THROW_ON_ERROR);
                if (!$request instanceof \stdClass) {
                    throw new \LogicException('The pull payload must be a JSON object');
                }
                $server->requests[] = ['sid' => $sid, 'reply' => $replyTo, 'payload' => get_object_vars($request)];
            }

            return [];
        };

        return [$transport, $watched, $client, $server];
    }

    private function iterator(NatsClient $client): PullConsumerIterator
    {
        return $client->jetStream()->pullConsumer('S', 'C')->setGroup('g')->setBatching(3)->setDepth(2)->setExpiresMs(30_000);
    }

    /** @param (\Closure(NatsMessage):void)|null $handler
     * @return Future<int>
     */
    private function startRun(PullConsumerIterator $iterator, ?\Closure $handler = null): Future
    {
        return $this->resources()->ownOperation($iterator->handle($handler ?? static function (): void {}), $iterator->stop(...), 'the pull run');
    }

    private function restartPin(PullConsumerIterator $iterator, Issue208PullLog $server): ?string
    {
        $before = count($server->requests);
        $run = $this->startRun($iterator);
        $this->waitUntil(static fn(): bool => count($server->requests) > $before);
        $pin = $server->requests[$before]['payload']['id'] ?? null;
        $iterator->stop();
        self::assertSame([0, null], self::settle($run));

        return $pin;
    }

    private function sendAndWait(ReconnectingTransport $transport, WatchedTransport $watched, string $frame): void
    {
        $reads = $watched->reads;
        $transport->pushFrame($frame);
        $this->waitUntil(static fn(): bool => $watched->reads > $reads && $watched->readsUnderWay === 1);
    }

    private function waitUntil(\Closure $condition): void
    {
        $deadline = hrtime(true) + 3_000_000_000;
        while (!$condition()) {
            self::assertLessThan($deadline, hrtime(true), 'the scripted scenario reached its gate');
            delay(0.001);
        }
    }

    /** @param Future<int> $future
     * @return array{?int, ?\Throwable}
     */
    private static function settle(Future $future): array
    {
        try {
            return [$future->await(new TimeoutCancellation(3)), null];
        } catch (\Throwable $failure) {
            return [null, $failure];
        }
    }

    private static function message(int $sid, string $payload, ?string $pin): string
    {
        $headers = "NATS/1.0\r\n" . ($pin === null ? '' : 'Nats-Pin-Id: ' . $pin . "\r\n") . "\r\n";

        return ReconnectingTransport::hmsgFrame('evt.s', $sid, $headers, $payload, '$JS.ACK.S.C.1.1.1.0.0');
    }

    private static function pinStatusFrame(string $reply, int $sid, int $code): string
    {
        return ReconnectingTransport::hmsgFrame($reply, $sid, sprintf("NATS/1.0 %d Nats-Pin-Id Mismatch\r\n\r\n", $code), '');
    }
}

final class Issue208PullLog
{
    /** @var list<array{sid: int, reply: string, payload: array<string,mixed>}> */
    public array $requests = [];
}

final class Issue208HandledLog
{
    /** @var list<string> */
    public array $payloads = [];
}
