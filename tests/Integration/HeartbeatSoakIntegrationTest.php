<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Integration;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\delay;

/**
 * Soak coverage for the heartbeat / read-loop interaction - timing behavior a static review and the
 * fast unit doubles cannot prove against a real socket. Uses a 1s ping interval so several heartbeat
 * ticks elapse during each test.
 */
final class HeartbeatSoakIntegrationTest extends TestCase
{
    use IntegrationTestBootstrap;

    protected function setUp(): void
    {
        $this->requireIntegrationEnabled();
    }

    public function testIdlePublisherStaysAliveViaHeartbeatSelfRead(): void
    {
        // A connected, publisher-only client that never calls processIncoming() must stay alive: the
        // heartbeat timer self-reads its own PONGs so outstandingPings cannot accumulate past maxPingsOut
        // and trip a false "server unresponsive" reconnect.
        $client = new NatsClient(new NatsOptions(
            servers: [$this->integrationServerUrl()],
            pingIntervalSeconds: 1,
            maxPingsOut: 2,
        ));
        $client->connect()->await();

        try {
            $client->publish('soak.idle', 'warmup')->await();

            // Idle for several heartbeat intervals WITHOUT pumping processIncoming(). delay() yields to
            // the event loop so the ping timer fires (~4-5 ticks); a broken self-read would reconnect
            // after ~maxPingsOut ticks.
            delay(4.5);

            self::assertSame(ConnectionState::Open, $client->state(), 'idle connection dropped despite the heartbeat self-read');
            self::assertSame(0, $client->statistics()->reconnects, 'idle connection reconnected (false unresponsive disconnect)');

            // Still usable afterwards.
            $client->publish('soak.idle', 'after-idle')->await();
            self::assertSame(ConnectionState::Open, $client->state());
        } finally {
            $client->disconnect()->await();
        }
    }

    public function testConcurrentHeartbeatAndProcessIncomingDeliverAllMessages(): void
    {
        // With an application processIncoming() loop AND the heartbeat self-read both wanting the socket,
        // the readInProgress guard must prevent overlapping reads (which would surface as a PendingReadError
        // and a spurious reconnect). Over several heartbeat ticks all messages must still be delivered and
        // the connection must stay up with zero reconnects.
        $client = new NatsClient(new NatsOptions(
            servers: [$this->integrationServerUrl()],
            pingIntervalSeconds: 1,
            maxPingsOut: 2,
        ));
        $client->connect()->await();

        /** @var list<string> $received */
        $received = [];
        $client->subscribe('soak.flow', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        $cancel = new DeferredCancellation();
        $loop = async(static function () use ($client, $cancel): void {
            $cancellation = $cancel->getCancellation();
            while (!$cancellation->isRequested()) {
                try {
                    $client->processIncoming($cancellation)->await();
                } catch (CancelledException) {
                    break;
                }
            }
        });

        try {
            $total = 10;
            for ($i = 0; $i < $total; $i++) {
                $client->publish('soak.flow', 'm' . $i)->await();
                delay(0.5); // ~5s total spans several 1s heartbeat ticks
            }

            // Allow the last few to drain.
            $deadline = $this->monotonic() + 2.0;
            while (count($received) < $total && $this->monotonic() < $deadline) {
                delay(0.05);
            }

            self::assertCount($total, $received, 'not all messages delivered under concurrent heartbeat + reads');
            self::assertSame(ConnectionState::Open, $client->state());
            self::assertSame(0, $client->statistics()->reconnects, 'overlapping heartbeat/user reads caused a reconnect');
        } finally {
            $cancel->cancel();
            $loop->await();
            $client->disconnect()->await();
        }
    }
}
