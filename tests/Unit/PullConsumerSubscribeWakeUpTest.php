<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\JetStream\Consumers\PullConsumerIterator;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

/** Initial pull-inbox setup honors run signals and preserves recorded server rejection errors (#198). */
final class PullConsumerSubscribeWakeUpTest extends TestCase
{
    use ReconnectScenarios;

    private const SUB = 'SUB _INBOX.JS.PULL.';
    private const PULL = '$JS.API.CONSUMER.MSG.NEXT.S.C';

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /** @return iterable<string, array{string, string}> */
    public static function subscribeSignals(): iterable
    {
        foreach (['stop', 'drain'] as $signal) {
            foreach (['joining', 'failed-sub'] as $shape) {
                yield "$signal/$shape" => [$signal, $shape];
            }
        }
    }

    /**
     * The run's stop() or drain() ends its inbox set-up while that set-up waits for a reconnect: joining one another
     * fiber started, held mid-dial, or recovering from its own SUB write that met a dead socket. The run returns 0 with
     * the dial still held, and once the connection is back no SUB of the abandoned set-up is replayed and no pull goes
     * out. Up to 2.25.8 the run waited for the reconnect, or failed with the subscribe's timeout.
     */
    #[DataProvider('subscribeSignals')]
    public function testASignalEndsSetupBeforeRecoveryIsReleased(string $signal, string $shape): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->probeClient($transport);
        $iterator = self::iterator($client);
        $reader = $shape === 'joining' ? $this->heldRecovery($client, $transport) : null;
        if ($shape === 'failed-sub') {
            $transport->holdNextDial();
            $transport->failNextWriteContaining(self::SUB);
        }
        $run = $this->startRun($iterator);
        $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 2);
        $this->checkpoint(); // Let the setup actually park before signalling it.
        $iterator->{$signal}();

        [$count, $error] = self::outcome($run);
        self::assertNull($error, $error === null ? '' : $error::class . ': ' . $error->getMessage());
        self::assertSame(0, $count);
        self::assertSame(ConnectionState::Connecting, $client->state(), 'the dial is still held');
        self::assertSame([], self::pulls($transport));
        self::assertFalse($client->isSubscriptionActive(1));

        $transport->releaseDial();
        $this->waitUntilOpen($client);
        $reader?->await(new TimeoutCancellation(3));
        $client->flush()->await(new TimeoutCancellation(3));
        self::assertSame([], $transport->controlLinesStartingWith(self::SUB, 1), 'abandoned setup cannot be replayed');
        self::assertSame([], self::pulls($transport));
    }

    /** @return iterable<string, array{string}> */
    public static function signals(): iterable
    {
        yield 'stop' => ['stop'];
        yield 'drain' => ['drain'];
    }

    /**
     * A signal latched before the run's engine starts skips the inbox set-up altogether: no SUB, no pull, and no
     * subscribe started, so the client's first subscription id is still free.
     */
    #[DataProvider('signals')]
    public function testALatchedSignalSkipsSetup(string $signal): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->probeClient($transport);
        $iterator = self::iterator($client);
        $run = $this->startRun($iterator);
        $iterator->{$signal}(); // No suspension since handle(): the engine has not started.
        self::assertSame(0, $run->await(new TimeoutCancellation(3)));
        self::assertSame([], $transport->controlLinesStartingWith(self::SUB));
        self::assertSame([], self::pulls($transport));
        self::assertSame(1, $client->subscribe('probe', static function (): void {})->await(new TimeoutCancellation(3)), 'the skipped set-up never started a subscribe: the first subscription id is still free');
    }

    /** A signal with no run active changes nothing for the next run (#189): it subscribes, pulls and delivers. */
    #[DataProvider('signals')]
    public function testASignalOutsideARunDoesNotStopTheNextRun(string $signal): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->probeClient($transport);
        $this->answerOne($transport);
        $iterator = self::iterator($client)->setIterations(1);
        $iterator->{$signal}();
        $messages = [];
        $run = $this->startRun($iterator, static function (NatsMessage $message) use (&$messages): void {
            $messages[] = $message->payload;
        });
        self::assertSame(1, $run->await(new TimeoutCancellation(3)));
        self::assertSame(['m1'], $messages);
    }

    /**
     * The SUB write failed and the reconnect has already replayed the inbox, its Reconnected listener held: the signal
     * ends the run with 0, and the replayed SUB is released by exactly one UNSUB on the new session.
     */
    #[DataProvider('signals')]
    public function testASignalDuringTheReconnectedListenerReleasesTheReplayedInbox(string $signal): void
    {
        $transport = new ReconnectingTransport();
        $entered = new DeferredFuture();
        $release = new DeferredFuture();
        $letGo = static function () use ($release): void {
            if (!$release->isComplete()) {
                $release->complete();
            }
        };
        $this->resources()->onRelease($letGo, 'Reconnected listener');
        $keepAlive = $this->resources()->ownCallback(EventLoop::delay(30, static function (): void {}));
        $client = $this->probeClient($transport, listener: static function (ConnectionEvent $event) use ($entered, $release): void {
            if ($event === ConnectionEvent::Reconnected) {
                $entered->complete();
                $release->getFuture()->await();
            }
        });
        $transport->failNextWriteContaining(self::SUB);
        $iterator = self::iterator($client);
        $run = $this->startRun($iterator);
        try {
            $entered->getFuture()->await(new TimeoutCancellation(3));
            $iterator->{$signal}();
            [$count, $error] = self::outcome($run);
            self::assertNull($error, $error === null ? '' : $error::class . ': ' . $error->getMessage());
            self::assertSame(0, $count);
            self::assertSame(ConnectionState::Open, $client->state());
            self::assertFalse($client->isSubscriptionActive(1));
            $this->waitUntil(static fn(): bool => $transport->controlLinesStartingWith('UNSUB ', 1) !== []);
            self::assertCount(1, $transport->controlLinesStartingWith(self::SUB, 1));
            self::assertSame(['UNSUB 1'], $transport->controlLinesStartingWith('UNSUB ', 1));
            self::assertSame([], self::pulls($transport));
        } finally {
            $letGo();
            EventLoop::cancel($keepAlive);
        }
        $this->checkpoint();
        $client->flush()->await(new TimeoutCancellation(3));
        self::assertSame(['UNSUB 1'], $transport->controlLinesStartingWith('UNSUB ', 1), 'no duplicate release');
    }

    /**
     * A latched signal does not turn a failure of the connection into a normal end. With reconnect off, the inbox's
     * SUB write fails and the connection closes for good; the Closed listener queues the stop or drain, which lands
     * once the subscribe has failed but before the run has seen it. The run still fails with the subscribe's
     * ConnectionException: only the set-up's budget ended by a signal (a TimeoutException) is a normal end.
     */
    #[DataProvider('signals')]
    public function testASignalAfterTheConnectionFailedDoesNotHideTheFailure(string $signal): void
    {
        $transport = new ReconnectingTransport();
        $signalled = new Issue198Signal();
        $client = $this->probeClient($transport, reconnect: false, listener: static function (ConnectionEvent $event) use ($signalled, $signal): void {
            if ($event === ConnectionEvent::Closed) {
                EventLoop::queue(static function () use ($signalled, $signal): void {
                    $signalled->runEndedFirst = $signalled->run?->isComplete();
                    $signalled->iterator?->{$signal}();
                });
            }
        });
        $transport->failNextWriteContaining(self::SUB);
        $signalled->iterator = self::iterator($client);
        $signalled->run = $this->startRun($signalled->iterator);

        [$count, $error] = self::outcome($signalled->run);
        self::assertFalse($signalled->runEndedFirst, 'the signal came before the run had seen the failure');
        self::assertNull($count, 'the run did not end normally');
        self::assertInstanceOf(ConnectionException::class, $error);
        self::assertSame('Reconnect is disabled', $error->getMessage());
        self::assertSame([], self::pulls($transport));
    }

    /** @return iterable<string, array{string, bool}> */
    public static function writeSignals(): iterable
    {
        foreach (['stop', 'drain'] as $signal) {
            yield "$signal/before-acceptance" => [$signal, false];
            yield "$signal/after-acceptance" => [$signal, true];
        }
    }

    /**
     * The signal ends a set-up whose SUB write is held, before the server accepted it or with its completion held after:
     * the run returns 0, and once the write is released its UNSUB follows the SUB on the wire.
     */
    #[DataProvider('writeSignals')]
    public function testASignalEndsAHeldSubAndItsReleaseStaysOrdered(string $signal, bool $accepted): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->probeClient($transport);
        if ($accepted) {
            $transport->holdCompletionOfNextWriteContaining(self::SUB, 30);
        } else {
            $transport->stallNextWriteContaining(self::SUB, 30);
        }
        $iterator = self::iterator($client);
        $run = $this->startRun($iterator);
        $this->waitUntil(static fn(): bool => ($accepted ? $transport->completionsHeld() : $transport->writesStalled()) === 1);
        $iterator->{$signal}();
        [$count, $error] = self::outcome($run);
        self::assertNull($error, $error === null ? '' : $error::class . ': ' . $error->getMessage());
        self::assertSame(0, $count);
        self::assertFalse($client->isSubscriptionActive(1));
        self::assertSame([], self::pulls($transport));
        $transport->releaseStalledWrites();
        $transport->releaseHeldCompletions();
        $this->waitUntil(static fn(): bool => $transport->controlLinesStartingWith('UNSUB ') !== []);
        $lines = $transport->controlLines();
        $sub = $transport->controlLinesStartingWith(self::SUB);
        self::assertCount(1, $sub);
        self::assertSame(['UNSUB 1'], $transport->controlLinesStartingWith('UNSUB '));
        self::assertGreaterThan(array_search($sub[0], $lines, true), array_search('UNSUB 1', $lines, true));
        $client->flush()->await(new TimeoutCancellation(3));
    }

    /** @return iterable<string, array{string}> */
    public static function shapes(): iterable
    {
        yield 'joining' => ['joining'];
        yield 'failed-sub' => ['failed-sub'];
    }

    /** Without a signal the set-up still waits for the reconnect within its budget, then the run pulls and delivers. */
    #[DataProvider('shapes')]
    public function testSetupStillWaitsForRecoveryWithoutASignal(string $shape): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->probeClient($transport);
        $this->answerOne($transport);
        if ($shape === 'joining') {
            $this->heldRecovery($client, $transport);
        } else {
            $transport->holdNextDial();
            $transport->failNextWriteContaining(self::SUB);
        }
        $iterator = self::iterator($client)->setIterations(1);
        $messages = [];
        $run = $this->startRun($iterator, static function (NatsMessage $message) use (&$messages): void {
            $messages[] = $message->payload;
        });
        $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 2);
        $this->checkpoint();
        self::assertFalse($run->isComplete());
        $transport->releaseDial();
        self::assertSame(1, $run->await(new TimeoutCancellation(3)));
        self::assertSame(['m1'], $messages);
        self::assertCount(1, self::pulls($transport));
    }

    /**
     * Without a signal a set-up whose one budget runs out still fails the run with the subscribe's TimeoutException,
     * and the abandoned SUB is not replayed once the connection is back.
     */
    #[DataProvider('shapes')]
    public function testSetupTimeoutWithoutASignalStillFails(string $shape): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->probeClient($transport, 150);
        if ($shape === 'joining') {
            $this->heldRecovery($client, $transport);
        } else {
            $transport->holdNextDial();
            $transport->failNextWriteContaining(self::SUB);
        }
        $run = $this->startRun(self::iterator($client));
        [$count, $error] = self::outcome($run);
        self::assertNull($count);
        self::assertInstanceOf(TimeoutException::class, $error);
        self::assertStringContainsString('Subscribe to "_INBOX.JS.PULL.', $error->getMessage());
        self::assertFalse($client->isSubscriptionActive(1));
        $transport->releaseDial();
        $this->waitUntilOpen($client);
        self::assertSame([], $transport->controlLinesStartingWith(self::SUB, 1));
        self::assertSame([], self::pulls($transport));
    }

    /**
     * stop() or drain() and then handle() again while the first run's set-up waits for a reconnect: the first run still
     * ends with 0, and only the second subscribes, pulls and delivers once the connection is back.
     */
    #[DataProvider('signals')]
    public function testRestartDuringSetupDoesNotEraseTheEarlierSignal(string $signal): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->probeClient($transport);
        $this->answerOne($transport);
        $this->heldRecovery($client, $transport);
        $iterator = self::iterator($client)->setIterations(1);
        $first = $this->startRun($iterator);
        $this->checkpoint();
        $iterator->{$signal}();
        $second = $this->startRun($iterator, static function (): void {});
        [$count, $error] = self::outcome($first);
        self::assertNull($error, $error === null ? '' : $error::class . ': ' . $error->getMessage());
        self::assertSame(0, $count);
        self::assertFalse($second->isComplete());
        $transport->releaseDial();
        self::assertSame(1, $second->await(new TimeoutCancellation(3)));
        self::assertCount(1, self::pulls($transport));
        self::assertCount(1, $transport->controlLinesStartingWith(self::SUB, 1));
    }

    /** @return iterable<string, array{bool, ?string}> */
    public static function rejections(): iterable
    {
        foreach (['permissions' => false, 'subscription limit' => true] as $reason => $limit) {
            foreach ([null, 'stop', 'drain'] as $signal) {
                yield $reason . '/' . ($signal ?? 'timeout') => [$limit, $signal];
            }
        }
    }

    /**
     * The server rejects the inbox (permissions, or the subscription limit) while the SUB's completion is held, another
     * fiber reading the -ERR. Without a signal the budget runs out and the run fails with the detailed rejection, the
     * timeout as its cause, not a bare timeout; with a stop or drain the run returns 0. The run ends before the write
     * is released, and the released SUB is followed by exactly one UNSUB.
     */
    #[DataProvider('rejections')]
    public function testARecordedRejectionSurvivesSetupTimeoutUnlessASignalEndsSetup(bool $limit, ?string $signal): void
    {
        $transport = new ReconnectingTransport();
        $reported = new DeferredFuture();
        $client = $this->probeClient($transport, $signal === null ? 300 : 20_000, errorListener: static function (\Throwable $error) use ($reported): void {
            if (!$reported->isComplete()) {
                $reported->complete($error->getMessage());
            }
        });
        $transport->answerPings = false;
        $transport->holdCompletionOfNextWriteContaining(self::SUB, 30);
        $transport->afterWrite = static function (string $bytes) use ($transport, $limit): void {
            if (str_starts_with($bytes, self::SUB)) {
                $inbox = explode(' ', $bytes)[1];
                $reason = $limit ? 'maximum subscriptions exceeded' : 'Permissions Violation for Subscription to "' . $inbox . '"';
                $transport->pushFrame("-ERR '" . $reason . "'\r\n");
            }
        };
        $iterator = self::iterator($client);
        $run = $this->startRun($iterator);
        $this->waitUntil(static fn(): bool => $transport->completionsHeld() === 1);
        $client->readIncomingForOperation(new TimeoutCancellation(3), alwaysReport: true)->await();
        self::assertStringContainsString($limit ? 'maximum subscriptions exceeded' : 'Permissions Violation', $reported->getFuture()->await(new TimeoutCancellation(3)));
        if ($signal !== null) {
            $iterator->{$signal}();
        }

        [$count, $error] = self::outcome($run);
        if ($signal === null) {
            self::assertNull($count);
            self::assertInstanceOf(JetStreamException::class, $error);
            self::assertStringContainsString($limit ? 'maximum subscriptions exceeded' : 'Permissions Violation', $error->getMessage());
            self::assertInstanceOf(TimeoutException::class, $error->getPrevious());
        } else {
            self::assertNull($error);
            self::assertSame(0, $count);
        }
        self::assertSame(1, $transport->completionsHeld(), 'the run ends before the write is released');
        self::assertFalse($client->isSubscriptionActive(1));
        self::assertSame([], self::pulls($transport));
        $transport->answerPings = true;
        $transport->releaseHeldCompletions();
        $this->waitUntil(static fn(): bool => $transport->controlLinesStartingWith('UNSUB ') !== []);
        self::assertSame(['UNSUB 1'], $transport->controlLinesStartingWith('UNSUB '));
        // Supply the held SUB's deliberately suppressed PONG, then check the next slot too.
        $transport->pushFrame("PONG\r\n");
        $client->flush()->await(new TimeoutCancellation(3));
    }

    private function probeClient(ReconnectingTransport $transport, int $budgetMs = 20_000, ?\Closure $listener = null, ?\Closure $errorListener = null, bool $reconnect = true): NatsClient
    {
        $client = $this->own(new NatsClient(new NatsOptions(
            requestTimeoutMs: $budgetMs,
            connectTimeoutMs: 10_000,
            pingIntervalSeconds: 0,
            reconnectEnabled: $reconnect,
            maxReconnectAttempts: 100,
            reconnectDelayMs: 5,
            reconnectMaxDelayMs: 20,
            reconnectJitterMs: 0,
            connectionListener: $listener,
            errorListener: $errorListener,
        ), $transport));
        $this->resources()->onRelease(static function () use ($transport): void {
            $transport->releaseDial();
            $transport->releaseStalledWrites();
            $transport->releaseHeldCompletions();
        }, 'transport gates');
        $client->connect()->await(new TimeoutCancellation(3));

        return $client;
    }

    /** @return Future<int> */
    private function heldRecovery(NatsClient $client, ReconnectingTransport $transport): Future
    {
        return $this->resources()->ownOperation($this->startRecoveryHeldMidDial($client, $transport), label: 'recovery reader');
    }

    private static function iterator(NatsClient $client): PullConsumerIterator
    {
        return $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(500);
    }

    /** @param (\Closure(NatsMessage): void)|null $handler
     *  @return Future<int>
     */
    private function startRun(PullConsumerIterator $iterator, ?\Closure $handler = null): Future
    {
        return $this->resources()->ownOperation($iterator->handle($handler ?? static function (): void {
            self::fail('the stopped run must not receive a message');
        }), $iterator->stop(...), 'pull run');
    }

    /** @param Future<int> $run
     *  @return array{?int, ?\Throwable}
     */
    private static function outcome(Future $run): array
    {
        try {
            return [$run->await(new TimeoutCancellation(1)), null];
        } catch (\Throwable $error) {
            return [null, $error];
        }
    }

    private function checkpoint(): void
    {
        $checkpoint = new DeferredFuture();
        $this->resources()->ownCallback(EventLoop::defer(static fn() => $checkpoint->complete()));
        $checkpoint->getFuture()->await(new TimeoutCancellation(3));
    }

    private function answerOne(ReconnectingTransport $transport): void
    {
        $transport->responder = static function (string $subject, ?string $replyTo) use ($transport): array {
            if ($subject !== self::PULL || $replyTo === null) {
                return [];
            }
            $sid = $transport->sidFor($replyTo);
            if ($sid === null) {
                throw new \RuntimeException('pull without a subscribed inbox');
            }

            return [ReconnectingTransport::msgFrame('evt.s', $sid, 'm1', '$JS.ACK.S.C.1.1.1.0.0')];
        };
    }

    /** @return list<string> */
    private static function pulls(ReconnectingTransport $transport): array
    {
        return $transport->controlLinesStartingWith('PUB ' . self::PULL);
    }
}

/** What the Closed listener of {@see PullConsumerSubscribeWakeUpTest::testASignalAfterTheConnectionFailedDoesNotHideTheFailure()} needs. */
final class Issue198Signal
{
    public ?PullConsumerIterator $iterator = null;

    /** @var Future<int>|null */
    public ?Future $run = null;

    public ?bool $runEndedFirst = null;
}
