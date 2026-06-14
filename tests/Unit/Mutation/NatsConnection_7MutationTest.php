<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\AuthenticationException;
use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;
use Psr\Log\LoggerInterface;

/**
 * Targeted mutation-killing tests for src/Connection/NatsConnection.php (chunk 7).
 *
 * Each test pins the exact observable behaviour a specific mutant would change: the bytes/events a
 * recording logger captures, the events a listener receives, a thrown exception, a dial count, or
 * the redelivery of a buffered message. All async work is driven cooperatively against the in-process
 * FakeTransport — no sockets, no timers, no wall-clock.
 */
final class NatsConnection_7MutationTest extends TestCase
{
    private const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    /**
     * A PSR-3 logger that records every log() invocation as [level, message, context].
     *
     * @return LoggerInterface&object{records: list<array{level: string, message: string, context: array<mixed>}>}
     */
    private function recordingLogger(): LoggerInterface
    {
        return new class extends AbstractLogger {
            /** @var list<array{level:string, message:string, context:array<mixed>}> */
            public array $records = [];

            public function log($level, string|\Stringable $message, array $context = []): void
            {
                $this->records[] = [
                    'level' => (string) $level,
                    'message' => (string) $message,
                    'context' => $context,
                ];
            }
        };
    }

    /**
     * emitEvent() info path (no error): a successful connect logs the Connected lifecycle event with
     * the "NATS connection " prefix and a context array carrying the 'event' key.
     *
     * kills ArrayItem @ 1861         (['event' => name] -> ['event' > name])
     * kills ArrayItemRemoval @ 1861  (['event' => name] -> [])
     */
    public function testConnectedEventIsLoggedWithEventContextKey(): void
    {
        $logger = $this->recordingLogger();
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false, logger: $logger), $transport);

        $connection->connect()->await();

        $info = array_values(array_filter(
            $logger->records,
            static fn(array $r): bool => $r['level'] === 'info' && $r['message'] === 'NATS connection Connected',
        ));
        self::assertCount(1, $info, 'Connected must be logged at info with the "NATS connection " prefix');
        // The context must retain the 'event' => 'Connected' key/value pair.
        self::assertArrayHasKey('event', $info[0]['context']);
        self::assertSame('Connected', $info[0]['context']['event']);
    }

    /**
     * emitEvent() warning path (event carries an error): an auth failure during connect emits
     * Closed with the throwable. Pins the exact warning message, the 'event' context key, and that a
     * warning is logged at all.
     *
     * kills Concat @ 1859               ('NATS connection ' . name -> name . 'NATS connection ')
     * kills ConcatOperandRemoval @ 1859 (-> name  and  -> 'NATS connection ')
     * kills ArrayItem @ 1859            (['event' => name] -> ['event' > name])
     * kills ArrayItemRemoval @ 1859     (drop 'event' entry)
     * kills MethodCallRemoval @ 1859    (drop the warning() call entirely)
     */
    public function testClosedWithErrorIsLoggedAsWarningWithFullMessageAndContext(): void
    {
        $logger = $this->recordingLogger();
        $transport = new FakeTransport([self::INFO, "-ERR Authorization Violation\r\n"]);
        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false, logger: $logger), $transport);

        try {
            $connection->connect()->await();
            self::fail('Expected AuthenticationException');
        } catch (AuthenticationException) {
            // expected
        }

        $warnings = array_values(array_filter(
            $logger->records,
            static fn(array $r): bool => $r['level'] === 'warning',
        ));
        self::assertNotEmpty($warnings, 'a lifecycle event carrying an error must be logged at warning level');

        $closed = array_values(array_filter(
            $warnings,
            static fn(array $r): bool => isset($r['context']['event']) && $r['context']['event'] === 'Closed',
        ));
        self::assertCount(1, $closed, "context must carry 'event' => 'Closed'");
        // Exact prefix+name ordering: not 'ClosedNATS connection ', not 'Closed', not 'NATS connection '.
        self::assertSame('NATS connection Closed', $closed[0]['message']);
        self::assertArrayHasKey('exception', $closed[0]['context']);
        self::assertInstanceOf(\Throwable::class, $closed[0]['context']['exception']);
    }

    /**
     * emitError() log line: a recoverable server -ERR is logged via logger->log() with the exact
     * "NATS connection error: " prefix concatenated to the throwable message, plus the exception
     * context, while the connection stays open.
     *
     * kills Concat @ 1884               (prefix . msg -> msg . prefix)
     * kills ConcatOperandRemoval @ 1884 (-> msg  and  -> prefix)
     * kills ArrayItemRemoval @ 1884     (['exception' => err] -> [])
     * kills MethodCallRemoval @ 1884    (drop the log() call)
     */
    public function testEmitErrorLogsPrefixedMessageWithExceptionContext(): void
    {
        $logger = $this->recordingLogger();
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            "-ERR 'Permissions Violation for Subscription to foo'\r\n",
        ]);
        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false, logger: $logger), $transport);
        $connection->connect()->await();

        $connection->processIncoming()->await();

        $errorLogs = array_values(array_filter(
            $logger->records,
            static fn(array $r): bool => str_starts_with($r['message'], 'NATS connection error: '),
        ));
        self::assertCount(1, $errorLogs, 'emitError must log with the "NATS connection error: " prefix first');
        self::assertStringContainsString('recoverable error frame', $errorLogs[0]['message']);
        // The throwable message follows the prefix; exact full string pins both concat operands + order.
        self::assertSame(
            "NATS connection error: Server sent recoverable error frame: 'Permissions Violation for Subscription to foo'",
            $errorLogs[0]['message'],
        );
        self::assertArrayHasKey('exception', $errorLogs[0]['context']);
        self::assertInstanceOf(\Throwable::class, $errorLogs[0]['context']['exception']);
    }

    /**
     * handleServerInfoUpdate(): a second async INFO with NO connect_urls must NOT re-fire
     * DiscoveredServers. With && a discovery event is emitted only when the urls are both non-empty
     * AND changed; the || mutant would fire again for the empty-but-different case.
     *
     * kills LogicalAnd @ 1909 (&& -> ||)
     */
    public function testDiscoveredServersNotReemittedForEmptyConnectUrls(): void
    {
        $events = [];
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            // First async INFO advertises peers -> DiscoveredServers (knownConnectUrls becomes non-empty).
            'INFO {"server_id":"S1","version":"2.12.0","max_payload":1048576,"connect_urls":["10.0.0.2:4222"]}' . "\r\n",
            // Second async INFO carries NO connect_urls (empty). Real code: no event. || mutant: event.
            'INFO {"server_id":"S1","version":"2.12.0","max_payload":1048576}' . "\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(reconnectEnabled: false, connectionListener: static function (ConnectionEvent $e) use (&$events): void {
                $events[] = $e;
            }),
            $transport,
        );
        $connection->connect()->await();

        $connection->processIncoming()->await(); // first async INFO
        $connection->processIncoming()->await(); // second async INFO (empty connect_urls)

        $discovered = array_filter($events, static fn(ConnectionEvent $e): bool => $e === ConnectionEvent::DiscoveredServers);
        self::assertCount(1, $discovered, 'DiscoveredServers must fire exactly once (not for the empty second INFO)');
    }

    /**
     * handleServerInfoUpdate(): lame-duck is announced exactly once. Setting lameDuckAnnounced = true
     * latches it; the false mutant would re-announce LameDuck on every subsequent ldm INFO.
     *
     * kills TrueValue @ 1916 (lameDuckAnnounced = true -> false)
     */
    public function testLameDuckAnnouncedOnlyOnce(): void
    {
        $events = [];
        $ldm = 'INFO {"server_id":"S1","version":"2.12.0","max_payload":1048576,"ldm":true}' . "\r\n";
        $transport = new FakeTransport([self::INFO, "PONG\r\n", $ldm, $ldm]);

        $connection = new NatsConnection(
            // reconnectEnabled:false isolates the latch from the lame-duck auto-failover (#47).
            new NatsOptions(reconnectEnabled: false, connectionListener: static function (ConnectionEvent $e) use (&$events): void {
                $events[] = $e;
            }),
            $transport,
        );
        $connection->connect()->await();

        $connection->processIncoming()->await(); // first ldm INFO
        $connection->processIncoming()->await(); // second ldm INFO

        $lameDuck = array_filter($events, static fn(ConnectionEvent $e): bool => $e === ConnectionEvent::LameDuck);
        self::assertCount(1, $lameDuck, 'LameDuck must be announced exactly once');
    }

    /**
     * handleServerInfoUpdate(): lame-duck auto-failover only triggers when the pool has MORE THAN one
     * endpoint. With a single configured server, a lame-duck INFO must NOT start a reconnect dial.
     * The >= 1 mutant would dial on a single-server pool.
     *
     * kills GreaterThan @ 1922 (count(serverPool()) > 1 -> >= 1)
     */
    public function testLameDuckDoesNotFailoverWithSingleServerPool(): void
    {
        // A lame-duck INFO with no connect_urls keeps the pool at the single configured server.
        $ldm = 'INFO {"server_id":"S1","version":"2.12.0","max_payload":1048576,"ldm":true}' . "\r\n";
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            $ldm,
            // Spare handshake frames the >=1 mutant would consume during its (wrong) reconnect dial.
            self::INFO,
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(servers: ['nats://127.0.0.1:4222'], reconnectEnabled: true, reconnectDelayMs: 1, reconnectJitterMs: 0),
            $transport,
        );
        $connection->connect()->await();

        $connection->processIncoming()->await(); // lame-duck INFO

        // Real code: no failover -> exactly one dial. >=1 mutant: a second (reconnect) dial happens.
        self::assertCount(1, $transport->connectCalls, 'single-server pool must not auto-failover on lame-duck');
    }

    /**
     * validateSubject(): a wildcard token must `continue` to validate the REMAINING tokens, so an
     * embedded-wildcard token after a standalone wildcard is still rejected. The `break` mutant would
     * abandon validation and wrongly accept the subject.
     *
     * kills Continue_ @ 1963 (continue -> break)
     */
    public function testWildcardTokenStillValidatesLaterTokens(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);
        $connection->connect()->await();

        // First token '*' is a valid standalone wildcard; second token 'bad*' has an embedded wildcard.
        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Wildcards must occupy an entire token');

        $connection->subscribe('*.bad*', static function (NatsMessage $m): void {})->await();
    }

    /**
     * drainPendingForSid(): the finally clause MUST clear the per-sid dispatch guard even when a
     * handler throws, so a later message for the same sid is still delivered. The UnwrapFinally mutant
     * leaves dispatchingSids[$sid] set, permanently wedging that subscription.
     *
     * kills UnwrapFinally @ 2015 (finally{ unset(dispatchingSids) } removed)
     */
    public function testDispatchGuardClearedAfterHandlerThrows(): void
    {
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            "MSG updates 1 5\r\nfirst\r\n",  // first delivery: handler throws
            "MSG updates 1 6\r\nsecond\r\n", // second delivery: must still reach the handler
        ]);
        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);
        $connection->connect()->await();

        $delivered = [];
        $throwOnce = true;
        $connection->subscribe('updates', static function (NatsMessage $m) use (&$delivered, &$throwOnce): void {
            $delivered[] = $m->payload;
            if ($throwOnce) {
                $throwOnce = false;
                throw new \RuntimeException('handler boom');
            }
        })->await();

        // The throwing handler propagates out of the first read; catch it so the test continues.
        try {
            $connection->processIncoming()->await();
            self::fail('Expected the throwing handler to propagate');
        } catch (\RuntimeException $e) {
            self::assertSame('handler boom', $e->getMessage());
        }

        // Second read: the guard must have been cleared, so 'second' is dispatched.
        $connection->processIncoming()->await();

        self::assertSame(['first', 'second'], $delivered, 'dispatch guard must be cleared after a handler throws');
    }
}
