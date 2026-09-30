<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\Enum\SlowConsumerPolicy;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\ThrowingLogger;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;
use Psr\Log\LoggerInterface;

/**
 * What a read reports to the error listener about the frames it brought - a message a drop policy
 * discarded, a recoverable -ERR, a malformed async INFO - is reported once the whole chunk is queued, and a
 * logger that throws on it neither fails the read nor costs a message.
 *
 * These reports used to run in the middle of the chunk. A listener that read on the connection then had the
 * next read's messages delivered ahead of the rest of this one, and a throwing logger failed the read - under
 * DropOldest after the oldest message was gone and before the new one was queued, so that one was lost too.
 */
final class ReadReportTest extends TestCase
{
    use ReconnectScenarios;

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /**
     * The policy, the frames that make the report ("overflow": three messages for a subscription that
     * holds two), the report's text, and the level it is logged at.
     *
     * @return iterable<string, array{SlowConsumerPolicy, string, string, string}>
     */
    public static function reports(): iterable
    {
        yield 'DropOldest' => [SlowConsumerPolicy::DropOldest, 'overflow', 'dropped oldest message', 'debug'];
        yield 'DropNewest' => [SlowConsumerPolicy::DropNewest, 'overflow', 'dropped newest message', 'debug'];
        yield 'a recoverable -ERR' => [
            SlowConsumerPolicy::DropOldest,
            "-ERR 'Permissions Violation for Subscription to \"secret\"'\r\n",
            'Server sent recoverable error frame',
            'error',
        ];
        yield 'a malformed async INFO' => [SlowConsumerPolicy::DropOldest, "INFO {not-json\r\n", 'Discarding malformed async INFO frame', 'error'];
    }

    /**
     * An error listener that reads on the connection when it gets the report finds the rest of the read
     * that made it queued ahead of the next read: another subscription's messages keep their order.
     */
    #[DataProvider('reports')]
    public function testListenerThatReadsFindsTheRestOfTheReadQueuedFirst(SlowConsumerPolicy $policy, string $trigger, string $report, string $level): void
    {
        $transport = new ReconnectingTransport();
        $listener = new class {
            public ?NatsConnection $connection = null;
            public bool $readOn = false;
        };
        $connection = $this->connection($transport, $policy, static function (\Throwable $error) use ($listener, $report): void {
            if (!$listener->readOn && str_contains($error->getMessage(), $report)) {
                $listener->readOn = true;
                $listener->connection?->processIncoming()->await();
            }
        });
        $listener->connection = $connection;
        $slow = $connection->subscribe('slow', static function (): void {})->await();
        $orders = $this->recordingSubscription($connection, 'orders');
        $transport->pushFrame(self::framesFor($trigger, $slow) . ReconnectingTransport::msgFrame('orders', $orders->sid, 'x1'));
        // The next read: the one the listener makes.
        $transport->pushFrame(ReconnectingTransport::msgFrame('orders', $orders->sid, 'x2'));

        $connection->processIncoming()->await();

        self::assertTrue($listener->readOn, 'the listener got the report');
        self::assertSame(['x1', 'x2'], $orders->payloads);
    }

    /**
     * A logger that throws on the report neither fails the read nor keeps the report from the error
     * listener, and every message the policy keeps is delivered.
     */
    #[DataProvider('reports')]
    public function testLoggerThrowingOnTheReportNeitherFailsTheReadNorCostsAMessage(SlowConsumerPolicy $policy, string $trigger, string $report, string $level): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connection($transport, $policy, $recorder->errorListener(), new ThrowingLogger($report));
        $slow = $this->recordingSubscription($connection, 'slow');
        $orders = $this->recordingSubscription($connection, 'orders');
        $transport->pushFrame(self::framesFor($trigger, $slow->sid) . ReconnectingTransport::msgFrame('orders', $orders->sid, 'x1'));

        $connection->processIncoming()->await();

        self::assertCount(1, $recorder->errorsContaining($report));
        self::assertSame(['x1'], $orders->payloads);
        if ($trigger === 'overflow') {
            self::assertSame($policy === SlowConsumerPolicy::DropOldest ? ['s2', 's3'] : ['s1', 's2'], $slow->payloads);
        }
    }

    /** @return iterable<string, array{SlowConsumerPolicy, list<string>}> */
    public static function dropPolicies(): iterable
    {
        yield 'DropOldest' => [SlowConsumerPolicy::DropOldest, ['j2', 'j3']];
        yield 'DropNewest' => [SlowConsumerPolicy::DropNewest, ['j1', 'j2']];
    }

    /**
     * The same for a SubscriptionQueue's own buffer, which fills when the application does not poll it: a
     * logger that throws on its drop report neither fails the read that delivered the message nor costs a
     * message the policy keeps. The read used to fail with the logger's exception, and under DropOldest the
     * new message was lost with it.
     *
     * @param list<string> $kept
     */
    #[DataProvider('dropPolicies')]
    public function testLoggerThrowingOnAQueueDropNeitherFailsTheReadNorCostsAMessage(SlowConsumerPolicy $policy, array $kept): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = new NatsClient(
            new NatsOptions(
                connectTimeoutMs: 500,
                pingIntervalSeconds: 0,
                errorListener: $recorder->errorListener(),
                maxPendingMessagesPerSubscription: 2,
                slowConsumerPolicy: $policy,
                logger: new ThrowingLogger('Slow consumer'),
            ),
            $transport,
        );
        $this->opened[] = $client;
        $client->connect()->await();
        $queue = $client->subscribeQueue('jobs')->await();
        $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $queue->sid, 'j1') . ReconnectingTransport::msgFrame('jobs', $queue->sid, 'j2'));
        $client->processIncoming()->await();
        // Nobody polled the queue: its buffer is full when the next job arrives.
        $orders = new class {
            /** @var list<string> */
            public array $payloads = [];
        };
        $ordersSid = $client->subscribe('orders', static function (NatsMessage $message) use ($orders): void {
            $orders->payloads[] = $message->payload;
        })->await();
        $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $queue->sid, 'j3') . ReconnectingTransport::msgFrame('orders', $ordersSid, 'x1'));

        $client->processIncoming()->await();

        self::assertCount(1, $recorder->errorsContaining('Slow consumer on sid ' . $queue->sid));
        self::assertSame(1, $queue->droppedCount());
        self::assertSame(['x1'], $orders->payloads);
        self::assertSame($kept, array_map(static fn(NatsMessage $message): string => $message->payload, $queue->fetchAll(2)));
    }

    /** The report is logged once, at its level: a drop policy's at debug, the others at error. */
    #[DataProvider('reports')]
    public function testReportIsLoggedAtItsLevel(SlowConsumerPolicy $policy, string $trigger, string $report, string $level): void
    {
        $transport = new ReconnectingTransport();
        $logger = new class ($report) extends AbstractLogger {
            /** @var list<string> */
            public array $levels = [];

            public function __construct(private readonly string $needle) {}

            /** @param array<mixed> $context */
            public function log($level, string|\Stringable $message, array $context = []): void
            {
                if (str_contains((string) $message, $this->needle)) {
                    $this->levels[] = (string) $level;
                }
            }
        };
        $connection = $this->connection($transport, $policy, null, $logger);
        $slow = $connection->subscribe('slow', static function (): void {})->await();
        $transport->pushFrame(self::framesFor($trigger, $slow));

        $connection->processIncoming()->await();

        self::assertSame([$level], $logger->levels);
    }

    /** A connection whose subscriptions hold at most two messages. */
    private function connection(
        ReconnectingTransport $transport,
        SlowConsumerPolicy $policy,
        ?\Closure $errorListener,
        ?LoggerInterface $logger = null,
    ): NatsConnection {
        $connection = new NatsConnection(
            new NatsOptions(
                connectTimeoutMs: 500,
                pingIntervalSeconds: 0,
                errorListener: $errorListener,
                maxPendingMessagesPerSubscription: 2,
                slowConsumerPolicy: $policy,
                logger: $logger,
            ),
            $transport,
        );
        $this->opened[] = $connection;
        $connection->connect()->await();

        return $connection;
    }

    /**
     * A subscription that records the payloads delivered to it.
     *
     * @return object{sid: int, payloads: list<string>}
     */
    private function recordingSubscription(NatsConnection $connection, string $subject): object
    {
        $subscription = new class {
            public int $sid = 0;
            /** @var list<string> */
            public array $payloads = [];
        };
        $subscription->sid = $connection->subscribe($subject, static function (NatsMessage $message) use ($subscription): void {
            $subscription->payloads[] = $message->payload;
        })->await();

        return $subscription;
    }

    private static function framesFor(string $trigger, int $slowSid): string
    {
        if ($trigger !== 'overflow') {
            return $trigger;
        }

        return ReconnectingTransport::msgFrame('slow', $slowSid, 's1')
            . ReconnectingTransport::msgFrame('slow', $slowSid, 's2')
            . ReconnectingTransport::msgFrame('slow', $slowSid, 's3');
    }
}
