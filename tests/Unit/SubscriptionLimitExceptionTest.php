<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\SubscriptionLimitException;
use IDCT\NATS\Tests\Support\OwnsTestResources;
use IDCT\NATS\Tests\Support\SubscriptionLimitServer;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Amp\delay;

/**
 * The type of a subscription-limit failure (#242). The server's "-ERR 'maximum subscriptions exceeded'" used to reach
 * callers as a plain ConnectionException, from the read that met it and from the errors the client builds for a reply
 * inbox the -ERR dropped, so a caller could recognise the limit only by matching the message. It is now a
 * SubscriptionLimitException, a ConnectionException, with the same message.
 *
 * This class covers the -ERR itself: thrown by the operation whose read meets it, and handed to the error listener by a
 * read that reports it (the heartbeat's). The errors built for a dropped reply inbox are pinned where their scenarios
 * live: `MuxInboxRejectionTest` (requests), `JetStreamInboxRejectionTest` (fetch, Direct Get batch, a replayed inbox the
 * new server rejects, and the pull pipeline, which keeps its JetStreamException).
 *
 * Over `tests/Support/SubscriptionLimitServer.php`, which enforces the limit and answers in wire order. Each test but
 * the guards fails on 2.26.8, where the class does not exist.
 */
final class SubscriptionLimitExceptionTest extends TestCase
{
    use OwnsTestResources;

    private const LIMIT_ERR = "-ERR 'maximum subscriptions exceeded'\r\n";
    private const SERVER_ERR = "Server sent error frame: 'maximum subscriptions exceeded'";

    protected function tearDown(): void
    {
        $this->releaseOwnedResources();
    }

    /**
     * The server refuses a SUB beyond the limit and keeps the connection open. The flush whose read meets the -ERR fails
     * with it, typed, and with the message it always had; the connection stays Open.
     */
    public function testTheOperationWhoseReadMeetsTheLimitErrFailsWithTheTypedError(): void
    {
        $server = new SubscriptionLimitServer();
        $server->limit = 1;
        $client = $this->connect($server);
        $client->subscribe('app.one', static function (): void {})->await();
        // Refused: the -ERR waits on the socket for the next read.
        $client->subscribe('app.two', static function (): void {})->await();

        $error = self::failureOf($client->flush());

        self::assertSame(SubscriptionLimitException::class, $error::class, $error::class . ': ' . $error->getMessage());
        self::assertSame(self::SERVER_ERR, $error->getMessage());
        self::assertSame(['app.one'], array_values($server->heldSubscriptions()));
        self::assertSame(ConnectionState::Open, $client->state());
    }

    /**
     * The heartbeat's read meets the -ERR, with no operation reading: it reports the error to the listener instead of
     * throwing, and what the listener gets is the same typed error.
     */
    public function testAReadThatReportsTheLimitErrHandsTheListenerTheTypedError(): void
    {
        $server = new SubscriptionLimitServer();
        $server->limit = 1;
        /** @var list<\Throwable> $reported */
        $reported = [];
        $client = $this->connect($server, pingIntervalSeconds: 0.05, errorListener: static function (\Throwable $error) use (&$reported): void {
            $reported[] = $error;
        });
        $client->subscribe('app.one', static function (): void {})->await();
        $client->subscribe('app.two', static function (): void {})->await();

        // By reference: an arrow function would capture the empty list.
        $this->waitUntil(static function () use (&$reported): bool {
            return $reported !== [];
        });

        self::assertSame(SubscriptionLimitException::class, $reported[0]::class, $reported[0]::class . ': ' . $reported[0]->getMessage());
        self::assertSame(self::SERVER_ERR, $reported[0]->getMessage());
        self::assertSame(ConnectionState::Open, $client->state());
    }

    /**
     * The type tells the reason, not the connection's state. A server whose account limit is lowered below what the
     * connection holds sends the same -ERR and closes the connection right behind it: the operation that meets the
     * -ERR fails with the typed error all the same, and the EOF behind it still ends the connection.
     */
    public function testTheLimitErrOfAServerThatClosesTheConnectionBehindItIsTypedTheSame(): void
    {
        $server = new SubscriptionLimitServer();
        $client = $this->connect($server);
        $server->answerPings = false;
        $flushing = $client->flush();
        $this->waitUntil(static fn(): bool => count($server->controlLines('PING')) === 2);

        $server->pushFrame(self::LIMIT_ERR);
        $error = self::failureOf($flushing);
        $server->dropConnection();

        self::assertSame(SubscriptionLimitException::class, $error::class, $error::class . ': ' . $error->getMessage());
        self::assertSame(self::SERVER_ERR, $error->getMessage());
        $next = self::failureOf($client->flush());
        self::assertNotInstanceOf(SubscriptionLimitException::class, $next, 'the next operation fails on the closed connection: ' . $next->getMessage());
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Closed);
    }

    /** @return iterable<string, array{string}> */
    public static function otherServerErrors(): iterable
    {
        yield 'an error that keeps the connection open' => ["'Invalid Publish Subject'"];
        yield 'a permissions violation that keeps the connection open' => ["'Permissions Violation for Publish with Reply of \"_INBOX.x\"'"];
        yield 'an error that ends the connection' => ["'Stale Connection'"];
    }

    /**
     * Guard, passes on 2.26.8 too: any other -ERR the read meets is the plain ConnectionException it always was.
     */
    #[DataProvider('otherServerErrors')]
    public function testOtherServerErrorsKeepThePlainClass(string $serverError): void
    {
        $server = new SubscriptionLimitServer();
        $client = $this->connect($server);
        $server->answerPings = false;
        $flushing = $client->flush();
        $this->waitUntil(static fn(): bool => count($server->controlLines('PING')) === 2);

        $server->pushFrame('-ERR ' . $serverError . "\r\n");
        $error = self::failureOf($flushing);

        self::assertSame(ConnectionException::class, $error::class, $error::class . ': ' . $error->getMessage());
        self::assertSame('Server sent error frame: ' . $serverError, $error->getMessage());
    }

    private function connect(SubscriptionLimitServer $server, float $pingIntervalSeconds = 0, ?\Closure $errorListener = null): NatsClient
    {
        $client = $this->own(new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            reconnectEnabled: false,
            pingIntervalSeconds: $pingIntervalSeconds,
            errorListener: $errorListener,
        ), $server));
        self::assertInstanceOf(NatsClient::class, $client);
        $client->connect()->await();

        return $client;
    }

    /**
     * @param Future<mixed> $operation
     */
    private static function failureOf(Future $operation): \Throwable
    {
        try {
            $operation->await(new TimeoutCancellation(2));
        } catch (\Throwable $error) {
            return $error;
        }

        self::fail('expected the operation to fail');
    }

    private function waitUntil(\Closure $condition): void
    {
        $deadline = hrtime(true) + 2_000_000_000;
        while (!$condition()) {
            if (hrtime(true) > $deadline) {
                self::fail('condition not reached within 2 s');
            }

            delay(0.001);
        }
    }
}
