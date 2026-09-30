<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Integration;

use Amp\CancelledException;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use PHPUnit\Framework\TestCase;

use function Amp\delay;

/**
 * drain() and drainSubscription() against a live NATS server: the close is announced once and can be
 * answered with a reconnect, a handler draining its own subscription still gets what the server already
 * sent it, a drained queue-group member stays gone, across a reconnect too, and a drainSubscription()
 * issued while its client reconnects waits for the reconnect.
 */
final class DrainLifecycleIntegrationTest extends TestCase
{
    use IntegrationTestBootstrap;

    /**
     * drain() announces Closed once the drain is over, so a Closed listener can reconnect straight away,
     * and the reopened connection works. A disconnect() of that connection announces its own close.
     */
    public function testDrainAnnouncesClosedOnceAndAClosedListenerCanReconnectAgainstALiveServer(): void
    {
        $this->requireIntegrationEnabled();
        $recorder = new LifecycleRecorder();
        $record = $recorder->connectionListener();
        $holder = new class {
            public ?NatsClient $client = null;
            public bool $reconnected = false;
            public ?\Throwable $failure = null;
        };
        $listener = static function (ConnectionEvent $event) use ($holder, $record): void {
            $record($event, null);
            $client = $holder->client;
            if ($event !== ConnectionEvent::Closed || $client === null || $holder->reconnected) {
                return;
            }

            $holder->reconnected = true;
            try {
                $client->connect()->await();
            } catch (\Throwable $e) {
                $holder->failure = $e;
            }
        };
        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()], connectionListener: $listener));
        $holder->client = $client;
        $client->connect()->await();
        $client->subscribe('it.drain.closed.' . bin2hex(random_bytes(4)), static function (): void {})->await();

        $client->drain()->await();

        self::assertNull($holder->failure);
        self::assertSame(ConnectionState::Open, $client->state(), 'the Closed listener reconnected');
        $subject = 'it.drain.reopened.' . bin2hex(random_bytes(4));
        $received = new class {
            /** @var list<string> */
            public array $payloads = [];
        };
        $client->subscribe($subject, static function (NatsMessage $message) use ($received): void {
            $received->payloads[] = $message->payload;
        })->await();
        $client->publish($subject, 'after-reconnect')->await();
        $client->flush()->await();
        $this->pumpUntil($client, static fn(): bool => $received->payloads !== []);
        self::assertSame(['after-reconnect'], $received->payloads);

        $client->disconnect()->await();
        self::assertSame(
            [ConnectionEvent::Connected, ConnectionEvent::Closed, ConnectionEvent::Connected, ConnectionEvent::Closed],
            $recorder->events,
        );
    }

    /** drain() then disconnect() - a common shutdown idiom - announces the close once. */
    public function testDisconnectAfterADrainAnnouncesTheCloseOnceAgainstALiveServer(): void
    {
        $this->requireIntegrationEnabled();
        $recorder = new LifecycleRecorder();
        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()], connectionListener: $recorder->connectionListener()));
        $client->connect()->await();
        $client->subscribe('it.drain.shutdown.' . bin2hex(random_bytes(4)), static function (): void {})->await();

        $client->drain()->await();
        $client->disconnect()->await();

        self::assertSame(ConnectionState::Closed, $client->state());
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Closed], $recorder->events);
    }

    /**
     * A handler that drains its own subscription and awaits it still receives the messages the server
     * sent before it processed the UNSUB: the delivery under way hands them over, then the subscription
     * is removed.
     */
    public function testHandlerDrainingItsOwnSubscriptionGetsWhatTheServerAlreadySentAgainstALiveServer(): void
    {
        $this->requireIntegrationEnabled();
        $subject = 'it.drain.self.' . bin2hex(random_bytes(4));
        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();
        $publisher = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $publisher->connect()->await();

        try {
            $holder = new class {
                public int $sid = 0;
                /** @var list<string> */
                public array $payloads = [];
            };
            $holder->sid = $client->subscribe($subject, static function (NatsMessage $message) use ($client, $holder): void {
                $holder->payloads[] = $message->payload;
                if ($message->payload === 'm1') {
                    $client->drainSubscription($holder->sid)->await();
                }
            })->await();
            $client->flush()->await();

            for ($i = 1; $i <= 5; $i++) {
                $publisher->publish($subject, 'm' . $i)->await();
            }
            $publisher->flush()->await();

            $this->pumpUntil($client, static fn(): bool => !$client->isSubscriptionActive($holder->sid));

            self::assertSame(['m1', 'm2', 'm3', 'm4', 'm5'], $holder->payloads);
            self::assertFalse($client->isSubscriptionActive($holder->sid));
        } finally {
            $publisher->disconnect()->await();
            $client->disconnect()->await();
        }
    }

    /**
     * A drained queue-group member stays gone, also across a reconnect of its client: afterwards the other
     * member gets every message of the group, so no leftover server-side subscription takes a share.
     */
    public function testDrainedQueueMemberStaysGoneAcrossAReconnectAgainstALiveServer(): void
    {
        $this->requireIntegrationEnabled();
        $subject = 'it.drain.queue.' . bin2hex(random_bytes(4));
        [$draining, $transport] = $this->connectRecoverableClient();
        $other = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $other->connect()->await();
        $publisher = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $publisher->connect()->await();

        try {
            $counts = new class {
                public int $drained = 0;
                public int $other = 0;
            };
            $drainedSid = $draining->subscribe($subject, static function () use ($counts): void {
                $counts->drained++;
            }, 'workers')->await();
            $other->subscribe($subject, static function () use ($counts): void {
                $counts->other++;
            }, 'workers')->await();
            $draining->flush()->await();
            $other->flush()->await();

            $draining->drainSubscription($drainedSid)->await();
            // The client reconnects; the drained subscription must not come back with it.
            $this->takeServerDownFor($draining, $transport);
            $this->bringServerBackAfter($transport, 0.1);
            $deadline = $this->monotonic() + 5.0;
            while ($draining->state() !== ConnectionState::Open && $this->monotonic() < $deadline) {
                delay(0.01);
            }
            self::assertSame(ConnectionState::Open, $draining->state());
            self::assertSame(1, $draining->statistics()->reconnects);

            for ($i = 1; $i <= 20; $i++) {
                $publisher->publish($subject, 'm' . $i)->await();
            }
            $publisher->flush()->await();
            $this->pumpUntil($other, static fn(): bool => $counts->other >= 20);

            self::assertSame(20, $counts->other);
            self::assertSame(0, $counts->drained);
        } finally {
            $publisher->disconnect()->await();
            $other->disconnect()->await();
            $draining->disconnect()->await();
        }
    }

    /**
     * drainSubscription() issued while its client reconnects waits for the reconnect and then drains on the
     * new connection: it resolves with the connection open again, and the drained queue-group member is
     * not subscribed on the server afterwards - the other member gets every message of the group.
     */
    public function testDrainSubscriptionIssuedDuringAReconnectWaitsForItAgainstALiveServer(): void
    {
        $this->requireIntegrationEnabled();
        $subject = 'it.drain.waits.' . bin2hex(random_bytes(4));
        [$draining, $transport] = $this->connectRecoverableClient();
        $other = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $other->connect()->await();
        $publisher = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $publisher->connect()->await();

        try {
            $counts = new class {
                public int $drained = 0;
                public int $other = 0;
            };
            $drainedSid = $draining->subscribe($subject, static function () use ($counts): void {
                $counts->drained++;
            }, 'workers')->await();
            $other->subscribe($subject, static function () use ($counts): void {
                $counts->other++;
            }, 'workers')->await();
            $draining->flush()->await();
            $other->flush()->await();

            $this->takeServerDownFor($draining, $transport);
            $this->bringServerBackAfter($transport, 0.3);
            $start = $this->monotonic();
            $draining->drainSubscription($drainedSid)->await();

            self::assertGreaterThanOrEqual(0.2, $this->monotonic() - $start, 'it waited for the reconnect');
            self::assertSame(ConnectionState::Open, $draining->state());
            self::assertSame(1, $draining->statistics()->reconnects);
            self::assertFalse($draining->isSubscriptionActive($drainedSid));

            for ($i = 1; $i <= 20; $i++) {
                $publisher->publish($subject, 'm' . $i)->await();
            }
            $publisher->flush()->await();
            $this->pumpUntil($other, static fn(): bool => $counts->other >= 20);

            self::assertSame(20, $counts->other);
            self::assertSame(0, $counts->drained);
        } finally {
            $publisher->disconnect()->await();
            $other->disconnect()->await();
            $draining->disconnect()->await();
        }
    }

    /**
     * Reads until $done() holds, for at most five seconds.
     *
     * @param \Closure(): bool $done
     */
    private function pumpUntil(NatsClient $client, \Closure $done): void
    {
        $cancellation = new TimeoutCancellation(5.0);
        try {
            while (!$done()) {
                $client->processIncoming($cancellation)->await();
            }
        } catch (CancelledException) {
            // Window elapsed; the caller's assertions report it.
        }
    }
}
