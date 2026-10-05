<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\CancelledException;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\SlowConsumerException;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\OperationsThatRead;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;

use function Amp\async;
use function Amp\delay;

/**
 * A subscription handler that throws while an operation's read delivers to it (#173). Operations read the socket
 * themselves while they wait for a result of their own, so their reads deliver messages for every subscription.
 * Another subscription's handler that threw used to fail the operation with its exception, even when the result
 * had already arrived: a request() made inside a service endpoint failed with it, and the endpoint answered its own
 * requester with a HANDLER_ERROR. The failure is now reported through the error listener, the rest of the read is
 * delivered, and the operation completes. With NatsOptions::$handlerErrorsFailOperations it still fails the
 * operation, as before that option existed.
 */
final class HandlerFailureDuringOperationTest extends TestCase
{
    use OperationsThatRead;
    use ReconnectScenarios;

    private const FAILURE = 'poison handler';

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /** @return iterable<string, array{bool}> */
    public static function eitherWay(): iterable
    {
        yield 'operations report handler failures (default)' => [false];
        yield 'handler failures fail operations' => [true];
    }

    /**
     * The read that brings an operation its result also brings a message for a subscription whose handler throws,
     * and one for a later subscription. The operation completes with its result, the failure is reported once, and
     * the later subscription's message is delivered by the same read. The operation used to fail with the
     * handler's exception, its result lost, and the later message waited for another read.
     */
    #[DataProvider('operationsThatRead')]
    public function testOperationCompletesDespiteAnotherSubscriptionsHandlerFailure(string $operation): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->clientReportingTo($transport, $recorder);
        [$frames, $later] = $this->poisonAndLater($client);
        [$run, $expected] = $this->prepareOperation($operation, $client, $transport, $frames);

        self::assertSame($expected, $run());
        self::assertSame([self::FAILURE], $recorder->errorsContaining(self::FAILURE));
        self::assertSame(['u1'], $later->payloads, 'delivered by the same read');
    }

    /**
     * With handlerErrorsFailOperations the handler's exception fails the operation, as before that option existed,
     * and it is not reported on top.
     */
    #[DataProvider('operationsThatRead')]
    public function testOperationFailsWithAnotherSubscriptionsHandlerFailureWhenConfiguredTo(string $operation): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->clientReportingTo($transport, $recorder, failOperations: true);
        [$frames] = $this->poisonAndLater($client);
        [$run] = $this->prepareOperation($operation, $client, $transport, $frames);

        try {
            $run();
            self::fail('expected the handler failure');
        } catch (\RuntimeException $e) {
            self::assertSame(self::FAILURE, $e->getMessage());
        }

        self::assertSame([], $recorder->errorsContaining(self::FAILURE));
    }

    /**
     * The case #173 reported: an endpoint handler makes a request(), and the read that brings the reply also brings
     * a message whose handler throws. The endpoint answers with the reply, counts no error and keeps no last error,
     * and the failure is reported. It used to answer its requester with a HANDLER_ERROR and keep the other
     * handler's message as its last error, which handlerErrorsFailOperations still does.
     */
    #[DataProvider('eitherWay')]
    public function testARequestAServiceEndpointMakesCompletesDespiteAnotherSubscriptionsHandlerFailure(bool $failOperations): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->clientReportingTo($transport, $recorder, $failOperations);
        $poison = $this->poisonSubscription($client);
        $transport->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $subject === 'svc.backend' && $replyTo !== null
            ? [ReconnectingTransport::msgFrame('poison', $poison, 'p1') . implode('', $transport->replyFrame($replyTo, 'answer'))]
            : [];
        $service = $client->service('gateway', '1.0.0')
            ->addEndpoint('ask', 'svc.ask', static fn(NatsMessage $message): string => 'got:' . $client->request('svc.backend', $message->payload, 1_000)->await()->payload);
        $service->start()->await();
        $endpoint = $transport->sidFor('svc.ask');
        self::assertNotNull($endpoint);
        $transport->pushFrame(ReconnectingTransport::msgFrame('svc.ask', $endpoint, 'q', '_INBOX.caller.1'));

        $service->run(timeoutSeconds: 0.3)->await();

        $stats = $service->statsSnapshot()['endpoints'][0];
        if ($failOperations) {
            self::assertCount(1, $transport->controlLinesStartingWith('HPUB _INBOX.caller.1 '), 'answered with an error');
            self::assertSame(1, $stats['num_errors']);
            self::assertSame(self::FAILURE, $stats['last_error']);
            self::assertSame([], $recorder->errorsContaining(self::FAILURE));

            return;
        }

        self::assertCount(1, $transport->controlLinesStartingWith('PUB _INBOX.caller.1 '), 'answered with the reply');
        self::assertStringContainsString("got:answer\r\n", implode('', array_column($transport->writes, 'bytes')));
        self::assertSame(0, $stats['num_errors']);
        self::assertNull($stats['last_error']);
        self::assertSame([self::FAILURE], $recorder->errorsContaining(self::FAILURE));
    }

    /**
     * A handler that throws a CancelledException is reported like any other, and the request whose read called it
     * gets its reply. The request used to take that exception for the end of its own wait: it read on with its
     * reply left undelivered, and timed out.
     */
    public function testAHandlersCancelledExceptionDoesNotEndARequestsWait(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->clientReportingTo($transport, $recorder);
        $cancelling = $client->subscribe('cancelling', static function (): void {
            throw new CancelledException();
        })->await();
        $transport->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $subject === 'svc' && $replyTo !== null
            ? [ReconnectingTransport::msgFrame('cancelling', $cancelling, 'c1') . implode('', $transport->replyFrame($replyTo, 'pong'))]
            : [];

        self::assertSame('pong', $client->request('svc', 'ping', 500)->await()->payload);
        self::assertSame(['The operation was cancelled'], $recorder->errorsContaining('cancelled'));
    }

    /**
     * An operation whose read runs a reconnect gets its result when the replay brings it behind a message whose
     * handler throws: the delivery after the reconnect reports the failure and delivers the rest. It used to stop
     * at the failure, and the operation, whose read delivers nothing an earlier read left queued, waited for the
     * server's next bytes: this poll returned nothing once its timeout ended.
     */
    public function testAnOperationWhoseReadRunsAReconnectGetsItsResultBehindAHandlerFailure(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            reconnectDelayMs: 1,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
            errorListener: $recorder->errorListener(),
        ), $transport);
        $this->opened[] = $client;
        $client->connect()->await();
        $poison = $this->poisonSubscription($client);
        $queue = $client->subscribeQueue('jobs')->await();
        // The new connection answers the replay of the queue's SUB with the poisoned message, then the job.
        $transport->afterWrite = static function (string $bytes) use ($transport, $poison, $queue): void {
            if ($transport->epoch() === 1 && str_contains($bytes, 'SUB jobs')) {
                $transport->afterWrite = null;
                $transport->pushFrame(
                    ReconnectingTransport::msgFrame('poison', $poison, 'p1')
                    . ReconnectingTransport::msgFrame('jobs', $queue->sid, 'j1'),
                );
            }
        };
        $poll = async(static fn(): ?string => $queue->setTimeout(1.0)->next()?->payload);
        delay(0.05);

        $transport->dropConnection();

        self::assertSame('j1', $poll->await());
        self::assertSame([self::FAILURE], $recorder->errorsContaining(self::FAILURE));
    }

    /** @return iterable<string, array{\Throwable, bool}> */
    public static function handlerFailuresAndWhoseSubscription(): iterable
    {
        yield "an exception, in the operation's own subscription" => [new \RuntimeException('own handler'), true];
        yield 'an exception, in another subscription' => [new \RuntimeException('own handler'), false];
        yield "an overflow it lets escape, in the operation's own subscription" => [new SlowConsumerException(999), true];
        yield 'an overflow it lets escape, in another subscription' => [new SlowConsumerException(999), false];
    }

    /**
     * A handler of the operation's own subscription that throws still fails the operation, as an overflow of that
     * subscription does: it is the operation's own failure. Only another subscription's is reported instead.
     */
    #[DataProvider('handlerFailuresAndWhoseSubscription')]
    public function testOnlyAnotherSubscriptionsHandlerFailureIsReportedInsteadOfFailingTheOperation(\Throwable $failure, bool $own): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = new NatsConnection(new NatsOptions(
            connectTimeoutMs: 500,
            pingIntervalSeconds: 0,
            errorListener: $recorder->errorListener(),
        ), $transport);
        $this->opened[] = $connection;
        $connection->connect()->await();
        $failing = $connection->subscribe('failing', static function () use ($failure): void {
            throw $failure;
        })->await();
        $transport->pushFrame(ReconnectingTransport::msgFrame('failing', $failing, 'x'));

        $thrown = null;
        try {
            $connection->readIncomingForOperation(new TimeoutCancellation(1), $own ? $failing : $failing + 1)->await();
        } catch (\Throwable $e) {
            $thrown = $e;
        }

        self::assertSame($own ? $failure : null, $thrown);
        self::assertSame($own ? [] : [$failure->getMessage()], $recorder->errorsContaining($failure->getMessage()));
    }

    /**
     * A reported handler failure is logged at error level, as the failures a service's run() reports are, and a
     * logger that throws keeps it neither from the error listener nor the operation from its result.
     */
    public function testAReportedHandlerFailureIsLoggedAtErrorLevel(): void
    {
        $logger = new class extends AbstractLogger {
            /** @var list<string> */
            public array $levels = [];

            /** @param array<mixed> $context */
            public function log($level, string|\Stringable $message, array $context = []): void
            {
                if (str_contains((string) $message, 'poison handler')) {
                    $this->levels[] = (string) $level;

                    throw new \LogicException('the logger failed');
                }
            }
        };
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            pingIntervalSeconds: 0,
            errorListener: $recorder->errorListener(),
            logger: $logger,
        ), $transport);
        $this->opened[] = $client;
        $client->connect()->await();
        [$frames] = $this->poisonAndLater($client);
        [$run, $expected] = $this->prepareOperation('request', $client, $transport, $frames);

        self::assertSame($expected, $run());
        self::assertSame(['error'], $logger->levels);
        self::assertSame([self::FAILURE], $recorder->errorsContaining(self::FAILURE));
    }

    /**
     * Your own reads still throw a handler's exception, whatever the option says: processIncoming() is how an
     * application dispatches, and the failure is its own to handle.
     */
    #[DataProvider('eitherWay')]
    public function testYourOwnReadStillThrowsAHandlerFailure(bool $failOperations): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->clientReportingTo($transport, $recorder, $failOperations);
        [$frames] = $this->poisonAndLater($client);
        $transport->pushFrame($frames);

        try {
            $client->processIncoming()->await();
            self::fail('expected the handler failure');
        } catch (\RuntimeException $e) {
            self::assertSame(self::FAILURE, $e->getMessage());
        }

        self::assertSame([], $recorder->errorsContaining(self::FAILURE));
    }

    private function clientReportingTo(ReconnectingTransport $transport, LifecycleRecorder $recorder, bool $failOperations = false): NatsClient
    {
        $client = new NatsClient(
            new NatsOptions(
                connectTimeoutMs: 500,
                requestTimeoutMs: 2_000,
                pingIntervalSeconds: 0,
                errorListener: $recorder->errorListener(),
                handlerErrorsFailOperations: $failOperations,
            ),
            $transport,
        );
        $this->opened[] = $client;
        $client->connect()->await();

        return $client;
    }

    /** A subscription whose handler throws {@see self::FAILURE}; returns its sid. */
    private function poisonSubscription(NatsClient $client): int
    {
        return $client->subscribe('poison', static function (): void {
            throw new \RuntimeException(self::FAILURE);
        })->await();
    }

    /**
     * Frames for a subscription whose handler throws, then for a later subscription that records what it gets,
     * and that later subscription.
     *
     * @return array{string, object{sid: int, payloads: list<string>}}
     */
    private function poisonAndLater(NatsClient $client): array
    {
        $poison = $this->poisonSubscription($client);
        $later = new class {
            public int $sid = 0;
            /** @var list<string> */
            public array $payloads = [];
        };
        $later->sid = $client->subscribe('updates', static function (NatsMessage $message) use ($later): void {
            $later->payloads[] = $message->payload;
        })->await();

        return [
            ReconnectingTransport::msgFrame('poison', $poison, 'p1') . ReconnectingTransport::msgFrame('updates', $later->sid, 'u1'),
            $later,
        ];
    }
}
