<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\Enum\SlowConsumerPolicy;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\SlowConsumerException;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\SubscriptionLimitServer;
use IDCT\NATS\Tests\Support\WatchedTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/** Pull consumers report nonfatal server errors and preserve other failure policies (#209). */
final class PullConsumerServerErrorTest extends TestCase
{
    use ReconnectScenarios;

    private const PULL = '$JS.API.CONSUMER.MSG.NEXT.S.C';
    private const LIMIT = "-ERR 'maximum subscriptions exceeded'\r\n";

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /** @return iterable<string, array{string, bool}> */
    public static function unrelatedErrors(): iterable
    {
        foreach (['maximum subscriptions exceeded', 'Permissions Violation for Publish with Reply of "_R_.abc.def"',
            'Invalid Publish Subject', 'Permissions Violation', 'Permissions Violation for Subscription to "app.other"',
            'Permissions Violation for Publish to "app.other"'] as $error) {
            foreach ([false, true] as $finite) {
                yield ($finite ? 'finite/' : 'infinite/') . $error => [$error, $finite];
            }
        }
    }

    /**
     * A run (batch 3, depth 1) whose pull holds m-1 and m-2 meets an -ERR that answers nothing of its own: one the server
     * keeps the connection open for (the limit, a reply-subject or a bare permissions violation, Invalid Publish
     * Subject) or a recoverable one (a guard: reported before #209 too). The error goes to the error listener once, the
     * run neither ends nor hands over its partial batch, and m-3 completes it: the handler gets m-1 to m-3, the
     * connection Open with nothing reconnected. Up to 2.26.x the first kind ended the run with the read's
     * ConnectionException.
     */
    #[DataProvider('unrelatedErrors')]
    public function testAnUnrelatedNonfatalErrorIsReportedAndTheBatchContinues(string $error, bool $finite): void
    {
        [$transport, $watched, $client, $recorder, $server] = $this->fixture();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000);
        if ($finite) {
            $iterator->setIterations(1);
        }
        $handled = self::stringLog();
        $run = $this->resources()->ownOperation($iterator->handle(static function (NatsMessage $message) use ($handled, $iterator, $finite): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm-3' && !$finite) {
                $iterator->stop();
            }
        }), $iterator->stop(...), 'pull consumer run');
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        $this->send($transport, $watched, (int) $server->sid, 'm-1');
        $this->send($transport, $watched, (int) $server->sid, 'm-2');
        self::assertSame([], $handled->getArrayCopy());
        $transport->pushFrame("-ERR '{$error}'\r\n");
        $this->waitUntil(static fn(): bool => $run->isComplete() || $recorder->errorsContaining($error) !== []);
        self::assertFalse($run->isComplete(), 'an unrelated nonfatal server error must not end the run');
        self::assertSame([], $handled->getArrayCopy(), 'the nonfatal error must not retire a partial batch');
        self::assertCount(1, $recorder->errorsContaining($error));
        $transport->pushFrame(self::message((int) $server->sid, 'm-3'));
        self::assertSame(3, $run->await(new TimeoutCancellation(2)));
        self::assertSame(['m-1', 'm-2', 'm-3'], $handled->getArrayCopy());
        self::assertSame(ConnectionState::Open, $client->state());
        self::assertSame(0, $client->statistics()->reconnects);
    }

    /** @return iterable<string, array{bool}> */
    public static function modes(): iterable
    {
        yield 'infinite' => [false];
        yield 'finite' => [true];
    }

    /**
     * Over SubscriptionLimitServer (limit 2, holding app.one and the run's confirmed inbox) the application subscribes
     * app.two: the server rejects that SUB, the run's read meets the -ERR and reports it, and the run's pull then gets
     * m-1. The run used to end at the -ERR.
     */
    #[DataProvider('modes')]
    public function testAnotherApplicationsSubRejectionDoesNotEndTheConsumer(bool $finite): void
    {
        $server = new SubscriptionLimitServer();
        $server->limit = 2;
        $pulls = self::stringLog();
        $server->responder = static function (string $subject, ?string $reply) use ($pulls): array {
            if ($subject === self::PULL && $reply !== null) {
                $pulls[] = $reply;
            }

            return [];
        };
        $recorder = new LifecycleRecorder();
        $client = $this->own(new NatsClient(new NatsOptions(
            reconnectEnabled: false,
            pingIntervalSeconds: 0,
            errorListener: $recorder->errorListener(),
        ), $server));
        $client->connect()->await();
        $client->subscribe('app.one', static function (): void {})->await();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000);
        if ($finite) {
            $iterator->setIterations(1);
        }
        $handled = self::stringLog();
        $run = $this->resources()->ownOperation($iterator->handle(static function (NatsMessage $message) use ($handled, $iterator, $finite): void {
            $handled[] = $message->payload;
            if (!$finite) {
                $iterator->stop();
            }
        }), $iterator->stop(...), 'pull consumer run');
        $this->waitUntil(static fn(): bool => count($pulls) === 1 && count($server->heldSubscriptions()) === 2);
        $client->flush()->await(); // Confirms the guarded inbox before the unrelated SUB.
        $client->subscribe('app.two', static function (): void {})->await();
        $this->waitUntil(static fn(): bool => $run->isComplete() || $recorder->errorsContaining('maximum subscriptions exceeded') !== []);
        self::assertFalse($run->isComplete(), 'an unrelated SUB rejection must not end the run');
        $reply = $pulls[0];
        self::assertIsString($reply);
        $lastDot = strrpos($reply, '.');
        self::assertNotFalse($lastDot);
        $base = substr($reply, 0, $lastDot);
        $sid = array_search($base . '.*', $server->heldSubscriptions(), true);
        self::assertIsInt($sid);
        $server->pushFrame(self::message($sid, 'm-1'));
        self::assertSame(1, $run->await(new TimeoutCancellation(2)));
        self::assertSame(['m-1'], $handled->getArrayCopy());
        self::assertCount(1, $recorder->errorsContaining('maximum subscriptions exceeded'));
    }

    /**
     * At the limit (1, held by `fill`) the server rejects the run's own inbox and the run's own read meets the -ERR:
     * handle() fails with the JetStreamException naming the pull inbox, as README promises, the inbox released and the
     * connection Open. It used to fail with the read's bare ConnectionException.
     */
    #[DataProvider('modes')]
    public function testTheConsumersOwnInboxLimitRejectionHasTheJetStreamException(bool $finite): void
    {
        $server = new SubscriptionLimitServer();
        $server->limit = 1;
        $client = $this->own(new NatsClient(new NatsOptions(reconnectEnabled: false, pingIntervalSeconds: 0), $server));
        $client->connect()->await();
        $client->subscribe('fill', static function (): void {})->await();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000);
        if ($finite) {
            $iterator->setIterations(1);
        }
        $run = $this->resources()->ownOperation($iterator->handle(static function (): void {}), $iterator->stop(...), 'pull consumer run');
        $failure = $this->failureOf($run);
        self::assertInstanceOf(JetStreamException::class, $failure, 'own inbox rejection must name the pull inbox');
        self::assertStringContainsString('Pull consumer reply inbox "_INBOX.JS.PULL.', $failure->getMessage());
        self::assertSame(ConnectionState::Open, $client->state());
        self::assertSame(['fill'], array_values($server->heldSubscriptions()));
    }

    /** @return iterable<string, array{string, bool, bool}> */
    public static function mixedFailures(): iterable
    {
        foreach (['handler', 'handler error', 'overflow'] as $failure) {
            foreach ([false, true] as $errorFirst) {
                yield $failure . ($errorFirst ? '/ERR first' : '/ERR last') => [$failure, $errorFirst, false];
            }
            yield $failure . '/parsed prefix of corrupt chunk' => [$failure, true, true];
        }
    }

    /**
     * A chunk brings the limit -ERR and a failure the options make the run's own (another subscription's handler
     * throwing an exception or an Error, or its overflow), the -ERR first or last, or in the parsed prefix of a chunk
     * that ends in bytes that fail to parse. The run still ends with that failure, after the handler got m-1 and m-2;
     * the -ERR is reported once. Reporting it must not swallow the configured failure.
     */
    #[DataProvider('mixedFailures')]
    public function testReportingTheServerErrorDoesNotHideAConfiguredFailureInTheSameChunk(string $kind, bool $errorFirst, bool $corrupt): void
    {
        [$transport, $watched, $client, $recorder, $server] = $this->fixture($corrupt);
        $otherFailure = $kind === 'handler error' ? new \Error('other handler failed') : new \RuntimeException('other handler failed');
        $otherSid = $kind === 'overflow' ? $client->subscribeQueue('other')->await()->sid
            : $client->subscribe('other', static function () use ($otherFailure): void {
                throw $otherFailure;
            })->await();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000);
        $handled = self::stringLog();
        $run = $this->resources()->ownOperation($iterator->handle(static function (NatsMessage $message) use ($handled): void {
            $handled[] = $message->payload;
        }), $iterator->stop(...), 'pull consumer run');
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        $this->send($transport, $watched, (int) $server->sid, 'm-1');
        $this->send($transport, $watched, (int) $server->sid, 'm-2');
        $frames = ReconnectingTransport::msgFrame('other', $otherSid, 'b1');
        if ($kind === 'overflow') {
            $frames .= ReconnectingTransport::msgFrame('other', $otherSid, 'b2') . ReconnectingTransport::msgFrame('other', $otherSid, 'b3');
        }
        $reads = $watched->reads;
        $chunk = $errorFirst ? self::LIMIT . $frames : $frames . self::LIMIT;
        $transport->pushFrame($corrupt ? $chunk . "BROKEN\r\n" : $chunk);
        // A corrupt chunk first starts reads of the reconnect's replay. Those are not the pump carrying on.
        $this->waitUntil(static fn(): bool => $run->isComplete() || ($watched->reads > $reads
            && $watched->readsUnderWay === 1 && $client->state() === ConnectionState::Open));
        self::assertTrue($run->isComplete(), 'reporting the server error must not swallow a configured failure');
        $failure = $this->failureOf($run);
        if ($kind === 'overflow') {
            self::assertInstanceOf(SlowConsumerException::class, $failure);
            self::assertSame($otherSid, $failure->sid);
        } else {
            self::assertSame($otherFailure, $failure);
        }
        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy());
        self::assertCount(1, $recorder->errorsContaining('maximum subscriptions exceeded'));
        self::assertSame(ConnectionState::Open, $client->state());
        self::assertSame($corrupt ? 1 : 0, $client->statistics()->reconnects);
    }

    /** Guard: a fatal `Stale Connection` behind the limit -ERR in one chunk still ends the run with reconnect off. */
    #[DataProvider('modes')]
    public function testANonfatalErrorDoesNotHideAFatalErrorInTheSameChunk(bool $finite): void
    {
        [$transport, $watched, $client, $recorder, $server] = $this->fixture();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000);
        if ($finite) {
            $iterator->setIterations(1);
        }
        $handled = self::stringLog();
        $run = $this->resources()->ownOperation($iterator->handle(static function (NatsMessage $message) use ($handled): void {
            $handled[] = $message->payload;
        }), $iterator->stop(...), 'pull consumer run');
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        $this->send($transport, $watched, (int) $server->sid, 'm-1');
        $transport->pushFrame(self::LIMIT . "-ERR 'Stale Connection'\r\n");
        $failure = $this->failureOf($run);
        self::assertInstanceOf(ConnectionException::class, $failure);
        self::assertSame("Server sent error frame: 'Stale Connection'", $failure->getMessage());
        self::assertSame(['m-1'], $handled->getArrayCopy());
        self::assertSame(ConnectionState::Closed, $client->state());
        self::assertCount(1, $recorder->errorsContaining('maximum subscriptions exceeded'));
    }

    /** @return array{ReconnectingTransport, WatchedTransport, NatsClient, LifecycleRecorder, object{sid: ?int}} */
    private function fixture(bool $reconnect = false): array
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->own(new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            reconnectEnabled: $reconnect,
            pingIntervalSeconds: 0,
            maxPendingMessagesPerSubscription: 2,
            slowConsumerPolicy: SlowConsumerPolicy::Error,
            handlerErrorsFailOperations: true,
            slowConsumerErrorsFailOperations: true,
            errorListener: $recorder->errorListener(),
        ), $watched));
        $client->connect()->await();
        $server = new class {
            public ?int $sid = null;
        };
        $transport->responder = static function (string $subject, ?string $reply) use ($transport, $server): array {
            if ($subject === self::PULL && $reply !== null) {
                $server->sid = $transport->sidFor($reply);
            }

            return [];
        };

        return [$transport, $watched, $client, $recorder, $server];
    }

    private function send(ReconnectingTransport $transport, WatchedTransport $watched, int $sid, string $payload): void
    {
        $reads = $watched->reads;
        $transport->pushFrame(self::message($sid, $payload));
        $this->waitUntil(static fn(): bool => $watched->reads > $reads && $watched->readsUnderWay === 1);
    }

    private static function message(int $sid, string $payload): string
    {
        return ReconnectingTransport::msgFrame('evt.s', $sid, $payload, '$JS.ACK.S.C.1.1.1.0.0');
    }

    /** @return \ArrayObject<int, string> */
    private static function stringLog(): \ArrayObject
    {
        return new \ArrayObject();
    }

    /** @param Future<int> $run */
    private function failureOf(Future $run): ?\Throwable
    {
        try {
            $run->await(new TimeoutCancellation(2));

            return null;
        } catch (\Throwable $failure) {
            return $failure;
        }
    }
}
